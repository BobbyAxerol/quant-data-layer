# P18.4 Data Layer Phase C Packet

Status: DRAFT_HOLD_NOT_AUTHORIZED. Source/local candidate only, not a release certificate.
Phase B handover and journal: DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md, final
P18.4 Phase B entries. Architecture: DATA_LAYER_V2_KAFKA_NATIVE_ARCHITECTURE_REVIEW.md
section 18.13. Candidate application source: `43bbcf0209f49e9cc1af6802af8f8a207a53ee3c`.
Evidence root: `/home/bobby/.local/state/qdl-v2/p184-phaseb-20261007/evidence/`.
The JSON inventory there contains exact container names, config fingerprints and
rollback digests, without environment secrets; it is not a rollout-order runner.
This amended Markdown governs ordering/gates and supersedes the old pre-core
coherence prerequisite. No Phase C runtime or binding-mount action has run.

Offline follow-up: [paired report](p184-idr062-offline-pair.md), measured on existing
images without runtime changes. Receipt SHA256
`b9d14de49256e7fb15b80faa430c52a610f18d5aa83c1be6dfc88f85e4de4ecc`.
Snapshot workload x1.45; total BOOK messages x1.0413 on two ETH bindings;
observed core CPU .0600 -> .0627 vCPU. These are NOT whole-stack or TS acceptance.

## Readiness audit update - 2026-10-07

[Readiness report](p184-dl-release-readiness.md) and [active capacity envelope](p184-idr062-capacity-envelope.md)
are required inputs to this packet. They supersede any inference that the quiet
pair or old whole-stack certificate proves present resource headroom.

- Owner release dependency: **TS DR01-DR08, including DR07, plus broker delta**.
  Do NOT wait for construction of TS Paper features or reopen unrelated phases.
- Active pair: 270/312 top changes in about120s (135/156 per minute); rc2 source-time
  materialization delay p50/p95/max0/0/0ms, n270/312. This is NOT client latency.
  Snapshot count239 ->639; core9.81 ->10.51CPU-s; no delta-chain difference.
- Actual acquisition roster17BOOK bindings. Steady-state scenario170snapshots/s;
  observed-size estimate1.369MB/s protobuf, not a packet/replication byte ceiling.
  Query RPC rate is not multiplied automatically by snapshot publication rate.
- **Pre-action resource HOLD:** current full stack mean5.9618vCPU, p956.2197,
  max6.3726 over60 intervals/307.156s, before isolated replay. The existing5vCPU
  gate already fails; do not roll core then call rollback PASS against this
  failing baseline. No cap/budget increase is implied. Owner must resolve the
  measured resource prerequisite before authorizing this packet.
- Current correct-TLS snapshot reads119/120usable; one BNB/OKX TRADE refusal
  retained (4262ms event age, live session). No quiet-market root cause claimed.
- Stream1 overflow+4 in approximately10min; cache polling observed BOOK_DELTA
  up to22825ms while nearby Query reads remained fresh. Exact matched-watermark
  attribution/recovery and full delivery measurements belong in DR07. No Paper
  feature work is required to supply that handoff evidence. Do not label them
  fixed or merely observer artifacts; distinguish the possible PUBSUB
  observer lag from the separate GET cache observation.
- qdl-sdk2.1.0 is candidate only; actual TS writer package is2.0.7. Binding r22
  V2 remains mounted, V3 remains staged. No required endpoint replacement has
  been identified, and no interface change is authorized by this report.

After readiness/resource prerequisites and owner approval, retain the exact
**core-first -> Claude TS coherence -> other roles** order below. No action was
performed by this audit. Existing thresholds are unchanged: >=117/120 per venue,
whole-stack <=5vCPU, no five consecutive rising lag intervals. A future change
to an approved resource budget must be versioned before measuring, not chosen
after seeing candidate results. Preserve all negative evidence.

## Prerequisites and post-core gate

1. TS owner completes DR01-DR08 (including DR07) and broker delta, independently
   of TS Paper feature construction, with new DL findings consolidated
   here before a single release. Do not substitute prior E09 or synthetic tests.
2. Complete the IDR-062 offline paired report using the existing deployed and rc2
   Rust images, identical captured bytes/configuration and resource limits. Core
   CPU/RSS and BOOK_SNAPSHOT message/byte rates are measured; downstream ratios
   are estimates only. Offline core replay does NOT measure live Kafka lag or
   whole-stack CPU, and is not a TS coherence certificate. Freeze the paired report
   hash before runtime approval; no image rebuild for measurement-only changes.
3. TS coherence is explicitly a **POST-CORE-ROLLOUT** gate, not a prerequisite that
   the unchanged old core must pass. Claude measures 120 consecutive samples per
   venue at 1 Hz after the three cores have caught up. Require >=117/120 per venue
   (39/40 = 97.5%); do not pool venues, drop refused/time-out samples, substitute
   a later green window or relax exact book-top/QUOTE equality. Preserve exact-view
   rejection reasons; report BOOK p95 against the existing 2000 ms package cap.
   Independent channels can still diverge: offline improvement does not imply PASS.
4. Owner approves exact runtime packet, role names/digests, maintenance/refusal
   window and rollback. Roster is the reviewed **12** existing application roles;
   the old handoff count of 13 was incorrect and is resolved by this inventory.
   No inferred thirteenth role, V1 replacement or state initializer recreation.

## Images and scope

One Rust and one Python image from the same application SHA. Both builds completed. Python packaged checks and native binary/linkage checks
passed; neither image has been applied to a runtime role.

- Python: `sha256:77682eea5054ae9ddbd858323b083286669a55eeb4bc7bc065b04bae8854e32f`
- Rust: `sha256:69e3b2b214331a7ebb6a110dd8159c899bbbd6439aaf87293c8c68d145e3713d`

| Existing role | Candidate | Retained rollback image |
| --- | --- | --- |
| `ingestor_binance_usdm` | Rust | `sha256:0f6876e16e51600419e1f172aae596bed9c2d369d1afcef93d1da30c18f262ec` |
| `query_kn_1` | Python | `sha256:514506122111df8992a5dfac9dc9a3db7ccae202a398424f4dd5a033d2ddf9a9` |
| `query_kn_2` | Python | `sha256:514506122111df8992a5dfac9dc9a3db7ccae202a398424f4dd5a033d2ddf9a9` |
| `market_projector_1` | Rust | `sha256:a696cfba6ab383173607f96413c814f74c7abcc98a4c41f57c9c6e78a41bb70b` |
| `market_projector_2` | Rust | `sha256:a696cfba6ab383173607f96413c814f74c7abcc98a4c41f57c9c6e78a41bb70b` |
| `stream_kn_1` | Rust | `sha256:1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9` |
| `stream_kn_2` | Rust | `sha256:1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9` |
| `ingestor_okx_swap` | Rust | `sha256:0f6876e16e51600419e1f172aae596bed9c2d369d1afcef93d1da30c18f262ec` |
| `rust_core` | Rust | `sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023` |
| `rust_core_2` | Rust | `sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023` |
| `rust_core_3` | Rust | `sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023` |
| `binance_bar_edge` | Python | `sha256:f7351c3bda080a6d1266d49480dc65d7e590375f178252f858f87a04b1a4d685` |

Keep original command/entrypoint, user, mounts, network aliases, TLS, healthcheck,
restart policy, CPU/RAM and source/group/config identities. Compare normalized
Docker config against recorded fingerprints immediately before action; stop on
unexpected drift. Never use a default source-tree Compose to recreate KN roles.
`docker start` only restarts an existing stopped container; it does not change its
image. The approved future replacement must be rendered from actual private
runtime configuration, preserving these fields, not reconstructed by guessing.

## Binding V3 order

The current shared TS writer r59 image is `9150385e...`; its TS binding reader 2.1.0
is distinct from this release's qdl-sdk 2.1.0. Reverify supported V1/V2/V3 parsing
on every actual caller before a mount change. No change to TS source in this packet.

Use `trading-system.paper.stable-r22-v3.json` in the evidence root, not the generic
r27 renderer output. Both contain 60 equal products, but r27 changes release and
inventory hashes; it is **not approved for the existing writer**. The r22 V3 file
only changes schema/checksum and adds 60 manifest-derived request policies:

- release revision 22; consumer manifest/JWT revision **10 unchanged**;
- universal hash `b4e43688ef714e6cef28c0bf2e565b0fbd7b5c70703c9271e1189a9c5bf99afe`;
- V3 binding hash `a7a4380581a923223bf40a067fd4f718c2ccf0b23705abf216abc38f86de84bd`;
- V3 file hash `4bdaa49410190948a8d15ad07ffae5bd73ccb48cc327fd2936b36b815daba5dc`.

Sequence: confirm readers first -> stage immutable V3 file externally -> owner/TS
mount switch in its approved window -> same-identity no-order refresh proof ->
resume qualification. Preserve original V2 file unchanged for rollback. Do not
rewrite a bind-mounted live file in place or assume all consumers share this file.
No consumer manifest edit, JWT regeneration, global hash update or new entitlement.

## Proposed bounded rollout, after approval only

Preflight ISR/quorum, cache generations/checkpoints, actual consumer quality,
TLS expiry/SAN, image digests, supported bindings/cursors and per-role config diff.
Freeze pre-action inventory and typed refusals; keep active images as named rollback.

1. Roll **rust_core -> rust_core_2 -> rust_core_3**, one at a time, to the Rust
   candidate above. Query/Stream/projectors/ingestors/BAR edge remain unchanged.
   Between cores verify assignment, committed-offset progress, first cache apply
   and actual consumer eligibility, not just ISR or process health. Stop on an
   ownership, integrity, generation or identity violation.
2. **Hold all remaining roles. Claude measures TS coherence** on these new core
   outputs using the post-core gate above, preserving 120 samples for each venue.
   Collect Kafka lag and CPU on the same interval. This is a bounded maintenance
   handoff, not a zero-downtime or independent-HA certificate.
3. Only after that receipt PASS: Query replicas individually -> Stream replicas
   individually -> projectors individually -> ingestors individually -> BAR edge.
   Recheck application recovery and affected eligible reads after each role. Keep
   SDK/binding compatibility checks in the binding section; no blanket catalogue or C2 rerun.

### Core-stage rollback conditions (freeze before action)

- Coherence **<117/120 on either venue**; four failures already make the window
  impossible to pass and allow an early stop. A read error/timeout counts as failed,
  not as a missing sample. An incomplete window is NOT PASS; hold or rollback,
  never resume the other roles without a complete accepted receipt.
- Exceed the CPU budget: the existing **5.0 steady-state vCPU whole-DL serving-stack
  budget** remains the ceiling (`config/v2/kn-v220-candidate-budget.json`, cpu
  denominator and production_acceptance: prior measured 4.6687 vCPU / 374.4 s,
  not a measurement of this new candidate). Scope: Kafka/Redis, ingestors, cores, projectors, Query,
  Stream and BAR edge; TS writer reported separately. Record the exact accounting
  roster before action. Sample cgroup CPU counters every 10 s; two successive
  non-overlapping 30 s means >5.0 trigger rollback. If a tighter measured operational
  envelope is approved before action, use that lower frozen value. Never infer
  whole-stack consumption from the offline two-binding core result. If the old
  stack already exceeds this ceiling during preflight, HOLD before rollout rather
  than attribute it to the candidate. No quota change is authorized here.
- Kafka backlog rises on **six consecutive 10 s readings** (five strictly increasing
  intervals) for any affected raw/core group or canonical/projector group. Record
  per-partition high watermark minus committed offset, not message-age substitutes.
  Unknown/missing offsets, counter reset or a rebalance invalidates the measurement:
  HOLD and establish ownership first, not reinterpret an unknown lag as zero.
- Any new loss/duplicate/integrity/fencing failure or OOM also stops this packet.

On a triggered gate, stop further roles and restore every core already changed,
reverse rollout order, to
`sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023`
with its recorded original config/mounts. Do not reset Kafka offsets/cache or roll
unmodified readers. Confirm catch-up and consumer usable data on return. Retain the
failed interval and rollback receipt; a retry needs an explained cause, not a
fresh window chosen to replace failed evidence.

No shared-runtime load or restart in Phase B. Phase C inherits unchanged evidence;
run only packaging/affected protocol and consumer gates. Do not repeat whole C2
or the catalogue to discover errors. Real-provider quiet/stale refusals remain
separate from internal defects; no timestamp rewrite, hidden fallback or SLA lift.

## Rollback and non-changes

Stop the failing replacement before restoring its exact recorded prior digest and
configuration. Restore V2 binding before rolling a reader back if it lacks V3;
readers already supporting V3 may keep the old compatible file. Preserve Kafka
committed offsets, topics/ACL, state generations, cache, TLS and credentials.
No flush, reset, repair publication, DB changes or deleted operational volumes.
Check bounded catch-up and consumer usable view on return; old image health alone
is insufficient. D1 remains deferred: quiet Binance BOOK may legitimately go stale.
D3 remains separate: no independent ingestor/bar-edge/Redis/host HA claim.

## Publication and cleanup

After runtime acceptance and explicit approval: feature -> dev CI -> main release,
immutable tag/assets with source/image/config/evidence and supported SDK/binding
versions. Never mutate v2.2.2. These rc2 images are not published stable artifacts.
The two superseded rc1 images and Phase B build target have already been removed;
do not repeat that cleanup. Retain active, candidate and named rollback images.
New disposable tests use --rm and are removed after their evidence is recorded. No broad prune,
shared cache cleanup, volume/network deletion or canonical/user-hunk modification.
Record disk pre/post and running-role image/start/restart comparison. Feature
worktree remains active until reviewed/merged; no worktree cleanup before that.

## DR07 source convergence amendment (2026-10-07)

HOLD remains. The new L2 no-whole-book-clone source slice is unit/oracle tested
and paired offline in `p184-dr07-hardening-progress.md`, but NOT inside rc2.
Do not deploy rc2 and claim this optimization runs. After source review, converge
one immutable core candidate and check affected provider capture/packaging before
using the existing core-first packet. Keep baseline rollback digest and all
coherence/lag/integrity gates above unchanged. Microbenchmark allocation savings
are not whole-stack vCPU evidence; the old 5.962 vCPU baseline still fails5.

TS reset source58287fc is isolated in codex/dr07-reset-anchor for Claude review;
no shared writer recreation is authorized by this DL packet. Optional Redis-ACK
trace is a separate TS source slice, not evidence of current deployed ACK latency.


## DR07 RC3 checkpoint - 2026-10-07 (not execution authorization)

[Runtime closure report](p184-dr07-runtime-closure.md) supersedes candidate-resource
inferences, not prior evidence. New proposed Rust candidate is
`sha256:0d8da150820ed684bda084e92f714537cb40bd81e2e3a6bc93fa9a4b33538411`
from c33c90f, optimization63238bf; rc2 does not include that optimization.
Paired real capture:3.59->3.01CPU-seconds, equal delta chains,49/53 top changes
materialized at source-time0ms delay (not consumer latency). Current full-stack
baseline remains5.779832vCPU, p955.978048,n30/299.996s: prerequisite FAIL.
Do not execute the rc2 roster above or silently substitute rc3 into an old approval.
Proposed bounded-trial amendment must resolve old-runtime CPU prerequisite while
retaining final<=5vCPU, coherence>=117/120 each venue, identity/integrity fencing
and lag gates. No amendment applied or caps raised in this slice. Named rollback
images/mounts/TLS/offsets unchanged. Claude's TS worktree remains protected;
reset/ACK trace integration and real DR/broker receipts are still required.


## DR07 owner-approved core-only trial - 2026-10-07

Owner explicitly approved changing gate order to trial the candidate. This
supersedes ONLY the old-runtime<=5vCPU pre-action prerequisite for this bounded
trial, not the final5vCPU release gate, not DR/broker release evidence, not TS
source integration, and not permission to roll the other9application roles.
Exact sequence rust_core -> rust_core_2 -> rust_core_3. Candidate
sha256:0d8da150820ed684bda084e92f714537cb40bd81e2e3a6bc93fa9a4b33538411;
rollback sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023.
Render private compose from current inspect using existing kn_production_packet
helpers; preserve config/TLS/state/offsets/aliases/caps, only image changes.
Readiness is actual partition progress and usable consumer data, not process-up.

Freeze before first action: trial safety CPU boundary=5.978048vCPU (already
measured old-runtime p95), two consecutive non-overlapping30s whole-stack means
above it stop further steps and roll changed roles back in reverse order.
This boundary governs transitional safety only; after convergence300s mean<=5
remains required for promotion. OOM/restart loop, integrity/ownership failure,
missing ISR/quorum, five consecutive10s per-partition lag increases, or failure
to recover application progress within120s also stop/rollback. A failed final
CPU gate or post-core TS coherence<117/120 on either venue means trialFAIL and
rollback, not changing limits to keep the candidate. Preserve failed samples.
Consumer QUOTE/BOOK freshness/policies remain unchanged; observations use actual
Redis read path without shared writes/orders/subscriptions. Coherence result is
review input for Claude; no replacement of complete TS DR01-DR08 acceptance.
Observer begins before first lifecycle call; only this frozen scope is approved.
A successful trial does not authorize release or rolling remaining roles.

# P18.4 DR01/DR08 TRADE Incident Attribution

Date: 2026-10-08. Status: READONLY_ATTRIBUTION_COMPLETE /
INCIDENT_WINDOW_RECOVERY_NOT_MEASURED / PROPOSALS_NOT_EXECUTED.
This report is not a DR01-DR08 acceptance certificate. No implementation source
change, runtime mutation, new measurement, test execution or cleanup was
performed here.

## Scope And Provenance

Rules read: `/home/bobby/AGENTS.md` and repository `AGENTS.md`.
Main journal: [Data Layer plan](../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md),
sections "IDR-075 preparation and busy-window receipt" and "DR01/DR08 isolated
runtime evidence and TRADE attribution". Governing guide: [KN architecture
review](../DATA_LAYER_V2_KAFKA_NATIVE_ARCHITECTURE_REVIEW.md), sections 18.4,
18.13-18.14; [Phase C packet](p184-dl-phase-c-packet.md), 2026-10-08 amendment.
No permission to change delivery contracts or roll runtime is implied.

Evidence root: `/home/bobby/.local/state/qdl-v2/p184-codex-release-20261008/cpu`.
The existing observation covered 03:21:03-03:26:03 UTC, 31 CPU samples / 30
intervals / 299.992355 seconds. Rates below use their own counter timestamps.
Retained evidence SHA256, recomputed read-only:

| File relative to evidence root | SHA256 |
| --- | --- |
| `receipt.json` | `e1434fe1c534a1c20946779a89b5a53fd9013cc5378f0b88b923058bc6a24e80` |
| `overflow-attribution.json` | `24dd25eb08133a6e6d1bb586301b95cb99e479b5ff95c1cb1f4f4fe55e925147` |
| `rates-summary.json` | `1311726a644235960e193d657dae2e0ce94d6bb9d8e9ebab6b8220ddc2e3bc33` |
| `window/stream_kn_1-window.json` | `7021fb73b3694a520ae47320d2617470fbed01dff9af8b80a1091e19afb7e809` |
| `window/stream_kn_2-window.json` | `0dc1edd99145623d0130c6e09a77d07a7a40ab105cde2699798e83f02b786f82` |
| `window/inventory-before.json` | `4016c9a322bffcb6257c73472c61d6ebddca88e46a168ed36a76a8b28465fcd7` |

## Exact Subscription Evidence

All three closures belong to `trading-system.paper.stable`, feed `TRADE`,
interval null, with `coalesced=0`, `rejected_at_push=0`, `aged_out_at_read=0`.
Counters are subscription-lifetime totals, NOT the five-minute window totals.

| Subscription / venue | Closure UTC | Initial after | Live delivered | Replayed | Queued / bytes | Outcome |
| --- | --- | ---: | ---: | ---: | --- | --- |
| 6635 / Binance | 03:21:34.481092968 | 442388521 | 135298 | 2715 | 1000 / 5261765 | RESOURCE_EXHAUSTED |
| 6625 / OKX | 03:21:34.487181266 | 389823200 | 240841 | 1 | 1000 / 4939480 | RESOURCE_EXHAUSTED |
| 6651 / Binance retry | 03:22:01.676587057 | 442924424 | 0 | 10000 | 1000 / 5253332 | OUT_OF_RANGE |

Physical keys:

- Binance: `a953e16e-7138-5562-b5e8-c337a44d0b65/trade/binance-usdm-btcusdt-trade-stable-001`.
- OKX: `fb26214c-7b9b-5961-95b2-55154755af0f/trade/okx-swap-btcusdt-trade-stable-001`.

The first two queues reached 1000 records. The Binance retry handed off 10000
replay records without live delivery, consistent with the replay bound. Its
OUT_OF_RANGE status alone does not prove retention loss: backlog and retention
have distinct details but share that status. Recovery was not measured within
this incident observation window.

Later evidence: TS IDR-078 at commit
`4b1606608f3f27d6f32b3c9636f18b162a0389de` documents both BTC TRADE views fresh
(approximately 200ms), eligible and advancing at 04:10 UTC, with +910 / +1218
offsets over 10 seconds. See `INTEGRATION_DEFECT_REGISTER.md`, IDR-078, and
`TRADING_SYSTEM_UNIFIED_IMPLEMENTATION_PLAN.md`, P18.4 IDR-078, in that TS
revision. This later sample supports eventual recovery; it does not measure
immediate recovery time (RTO), reconstruct the incident-window recovery
timeline or prove the precise root cause of the stall. Offset advancement is
not a payload-throughput measurement. IDR-078's worker logging fix is recorded
as tested locally, not a deployed observability guarantee for this incident.

## Rates And Their Denominators

Formula: `(last counter - first counter) / elapsed timestamp seconds`.
These are replica aggregates across subscriptions, not BTC-specific rates.

| Replica / UTC interval | Seconds | Hub offer delta / rate per second | Live delivered delta / rate per second | Replay delta / rate per second |
| --- | ---: | --- | --- | --- |
| stream_kn_1 03:21:11.172417483-03:26:01.171030676 | 289.998613193 | 213282 / 735.46 | 161090 / 555.49 | 15901 / 54.83 |
| stream_kn_1 03:21:21.173973554-03:21:31.173673629 | 9.999700075 | 26103 / 2610.38 | 18924 / 1892.46 | 0 / 0 |
| stream_kn_1 03:21:31.173673629-03:21:41.170393711 | 9.996720082 | 23700 / 2370.78 | 10571 / 1057.45 | 0 / 0 |
| stream_kn_2 03:21:04.812892649-03:25:54.811353809 | 289.998461160 | 0 / 0 | 0 / 0 | 0 / 0 |

The third row encloses both overflow closures (+2). Stream 1 opened three
subscriptions over its retained window. Stream 2 had zero active subscribers
in retained samples but still read canonical records. Hub record rates were
762.88/s and 763.73/s respectively; do not sum replica reads as unique ingress.

`hub_offers` increments per physical-key subscriber before `offer` filtering
(`rust/qdl-stream-gateway/src/hub.rs:295-303`). It includes attempts later
rejected as duplicate, wrong product, stale, closed or overflowed. It is not
the accepted subscription-input counter. `SubscriptionCounters.offered`
increments after filtering, before capacity admission, including the failing
offer (`subscription.rs:196-246`), but is absent from the closure JSON
(`service.rs:948-974`) and periodic per-replica metrics (`main.rs:314-338`).

Therefore neither affected subscription has a measured offered or delivered
rate in this evidence. Lifetime delivery totals lack opening timestamps and
paired window counters. Dividing them by 300s, dividing replica rates by 50,
or treating aggregate offers minus delivery as lost records is invalid.

Kafka log-end snapshots cover 302.110102965s: canonical +287754 = 952.48056
offsets/s; raw +249628 = 826.28154 offsets/s, six partitions per topic. These
include transaction markers, not just payloads; partition offsets also mix
products and do not identify one subscription's traffic. CPU mean 4.882532,
p95 5.564052, max 5.870669 vCPU is not proof of 3000 payload/s capacity or
absence of brief stalls.

## Precise Stream Flow Control

References below are in `rust/qdl-stream-gateway/src/` unless stated otherwise.

1. `subscription.rs:190-266`: the shared live reader offers synchronously and
   never awaits subscriber capacity. Admission requires item space, per-queue
   byte space and replica byte budget. If no legal lifecycle coalescing can
   make space, mark overflow. TRADE is lossless by
   `rust/qdl-contracts/src/delivery.rs:38-45`.
2. `subscription.rs:273-295`: overflow takes precedence over queued records;
   this is termination/recovery, not draining the queue before failure.
3. `service.rs:44,472-499`: the subscription task sends through a two-slot
   Tokio mpsc channel. Tonic polls `ReceiverStream`; the memory charge drops
   when the mapped item is taken, not when the remote application acknowledges
   it. This budget does not count all downstream encoded/transport buffers.
4. `service.rs:985-1014`: send awaits local channel capacity with configurable
   `slow_consumer_after` (default 10000ms in `main.rs:212`), raced against
   authorization revocation. Success means local enqueue. Live `delivered`
   increments at `service.rs:1282-1287`; replay increments separately at
   `service.rs:1114-1123`. Neither counter proves socket transmission, SDK
   receipt, application processing or durable checkpoint.
5. HTTP/2/gRPC transport flow control can indirectly slow Tonic polling and
   fill that handoff; it does not pause this shared reader or canonical
   production. Successful gRPC writes need not have reached the network:
   [official gRPC flow-control guide](https://grpc.io/docs/guides/flow-control/).
   This evidence does not measure HTTP/2 windows or pinpoint a transport stall.
6. `service.rs:1021-1052,1313`: overflow attempts BACKPRESSURE/RATE_LIMITED
   control with resume token followed by RESOURCE_EXHAUSTED. Control enqueue
   and terminal-status enqueue each have a five-second timeout with ignored
   result. Logged closure is not proof the client received either signal.
   Queue overflow uses the task's last progress offset; send timeout has its
   separate `record.offset - 1` path (`service.rs:1084-1088`).
7. `service.rs:452-497,1138-1215`: registration admits live records while replay
   runs first; the live queue can fill during replay. The 10000 replay-event
   default is `main.rs:209`; `service.rs:766-789` maps backlog to OUT_OF_RANGE /
   CURSOR_EXPIRED with a fresh-snapshot detail, separately from retention and
   retryable replay time-limit outcomes.

No measured stage attribution separates client processing, transport pressure,
task scheduling, serialization and arrival bursts. The collector at
`/home/bobby/.local/state/qdl-v2/dr07-runtime-20261007/continuation/caller_resource_window.py:97`
kept numeric scalars only, excluding the queue/handoff histogram objects emitted
by `main.rs:332-333`. Even those histograms are aggregate; handoff timing wraps
delivery work, not a pure HTTP/2 stall timer. Missing data, persistent infinite
retry, client-applied throughput and immediate recovery time are not proved by
the incident window. The later IDR-078 sample supports eventual recovery as
described above, without resolving precise stall attribution.

## Proposal: Per-Subscription Delivery Mode

NOT IMPLEMENTED OR AUTHORIZED FOR RUNTIME. Add explicit per-subscription TRADE
delivery mode: omitted/default remains `LOSSLESS`; TRADE `LATEST_STATE` requires
an approved consumer/product capability. Existing QUOTE and other latest-state
modes remain unchanged. This is not a global LOSSLESS default. Do not change
canonical ingest, durable history or another subscriber's delivery. Current
mode is feed/lifecycle-derived, not a subscriber choice; preserve existing
request semantics for all other feeds.

TRADE latest-state opt-in would change current invariant 27 / guide 18.4.1 and
needs explicit domain approval, especially for execution, volume and audit
consumers. Define whether the consumer needs a latest-trade state projection
instead of the complete TRADE event feed. Preserve quality/authority/session
transitions, bounded bytes/items, identity/order, original clocks, and explicit
coalesced counts. Version and validate mode/cursor/replay semantics, rejecting
unauthorized modes or mismatched resumes. Never silently coalesce lossless
TRADE, book lifecycle or final BAR/revisions.

Required future tests: concurrent lossless/latest subscriptions on one product,
fast-reader independence, alternating transition preservation, bounded memory,
auth negatives, mode/cursor mismatch, cancellation and reconnect/recovery.
Instrument per-subscription offered/accepted/dequeued/local-enqueued/coalesced/
rejected counters with monotonic intervals and client received/applied progress.
Keep replay separate; capture queue/byte high-water and wait distributions.

## Proposal: Paired batch_wait_ms 25 -> 50

NOT RUN. Owner verified running Entrypoint:
`/usr/local/bin/qdl-realtime-core /runtime/core.json`.
Use `rust/qdl-kafka/src/bin/qdl-realtime-core.rs:326-335`: after the first input,
one `batch_deadline = collect_started + batch_wait_ms` is shared by all remaining
receives. `receive_batch_input` at line 220 enforces that deadline, including
already-ready receives. It does not reset the wait per record. The different
loop in `qdl-production-core.rs:529` is NOT the running binary and must not be
used to model this proposal. Commit is awaited at realtime-core.rs:473-475.
Thus 25 -> 50 extends the maximum additional collection window after the first
record, unless batch_size wins; it is not 50ms per record or a fixed total
end-to-end latency bound. More batching may reduce commit cost but increase
waiting, burst size and downstream queue pressure. Improvement is a hypothesis.

Use isolated paired runs over identical durably captured provider bytes and
arrival timing, fixed image, batch_size, partitions, quotas and consumers;
change only batch_wait_ms. Repeat with order reversal; do not combine with a
LATEST_STATE experiment. State replay provenance, not live-provider or HA
certification. Any later live comparison must record simultaneous real payload
rates/mix and acknowledge that two live windows are not identical workloads.

Before execution, record exact isolated roles/configs, retention, stop gates
and rollback to 25 in the main plan. Measure batch size, commits/s, commit and raw-age
latencies, venue-to-durable/client-applied p50/p95/p99, per-subscription input and
output, queue/handoff waits, overflow/replay/recovery, CPU/RAM/I/O and broker lag.
Require identity/order/decimal/time parity, no unexplained loss/duplicates/gaps,
unchanged freshness/latency gates and bounded resources before judging CPU.
No source/config edit, rollout, restart or measurement is authorized by this text.

## Inspection And Runtime Scope

Read-only commands used: scoped `cat`/`sed`/`rg`/`jq`, `sha256sum`,
`git --no-optional-locks status --short --branch`, `git worktree list --porcelain`,
and scoped `git diff` against recorded deployed revisions. Stream service,
subscription, hub, reporter and delivery-policy files have no diff from
`c8015cac822c8f2759dafabe9a56f97409c39c73`; realtime-core.rs has no diff from
`0e00c817289ce32ea767cea002c859814349fc72`. No test suite was run or claimed.
Rates were calculated with jq over retained JSON, not a new observation.

Canonical checkout: `/home/bobby/data_layer`, `main@5bf5cd540498b82959342c7cfb2ebeaac3007426`,
published v2.2.2. Registered feature checkout:
`/home/bobby/.worktrees/data-layer-p184-closure`,
`fix/p184-dl-closure@704552d8736905cdb73feec86f3f01d339d02cbb` at inspection.

Incident inventory, not freshly revalidated runtime: streams
`sha256:1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9`;
three cores `sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023`.
Full 17-role DL plus TS caller service/image/config hashes are in the hashed
inventory above. Journal records manifest/JWT10 and binding V2r22. Stable
v2.2.2 remains published; v2.2.3 preparation is under
`upgrade/evidence/releases/v2.2.3/`, not release approval. Retain active and
named rollback artifacts, including bar edge
`sha256:f7351c3bda080a6d1266d49480dc65d7e590375f178252f858f87a04b1a4d685`.
No cleanup or runtime restart occurred; no new images, caches or disposable
runtime resources were created. DR acceptance, incident-window RTO, broker and
release gates remain open; eventual recovery evidence is distinguished above.
Future instrumentation, contract changes or an exact isolated measurement
packet require review before execution.

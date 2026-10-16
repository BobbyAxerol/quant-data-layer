# Quant Data Layer v2.2.4 (DRAFT)

Status: **DRAFT - not certified, not published.** This file is the release-notes skeleton written during the
stage-100 #3 measurement window (host lock `claude-n1-opt-20261013`, 2026-10-16). It holds no gate result. Every
`<...>` placeholder is filled from receipts after stage-100 #3, the release image build and the equivalence check.
The full inventory, build plan and open items are in [release-preparation.json](release-preparation.json).

Updated 2026-10-16 ~11:30Z under host lock `claude-burst-b16-a1parity` (file edits only): SDK bump committed
(`aaa81a8`), release image recipe committed (`46f4628`, `scripts/build_qdl_release_images.py`), production changes
since `ef6e24b` recorded (TS writer B17, Kafka A' and C), TEST_ONLY burst results summarised.

Updated 2026-10-16 ~15:40Z under host lock `claude-g12-final` (another agent's G12 run; file edits only): Kafka B
applied, the first real production coordinator-load measurement, the temporary cleaner key applied and removed,
IDR-108, the burst closure, the final TS writer candidate, and a proof map per component. The ordered remaining
steps are in [FINAL_STEPS.md](FINAL_STEPS.md).

- Previous stable release: v2.2.3 (source `481fd67`, SDK 2.3.0).
- Publication path (unchanged): required remote CI on the final head, `fix/p184-dl-closure` -> `dev` PR, release PR
  `dev` -> `main`, tag `v2.2.4` on main. `.github/workflows/release.yml` checks the certificate status, the
  endpoint-report hash and the SDK wheel hash.
- Scope: the Data Layer release only. Not a Trading System release. Not HA (single host).

## Scope

83 commits since `481fd67` (`git rev-list --count 481fd67..87db215`), grouped by runtime area:

- **Core rebalance, rewind and Kafka client** (rust_core, shared `qdl-kafka`): pipelined transaction commit
  (IDR-094, a962a5c), K1/K3 stop intent and epoch abort (bcb0283), capacity step 1 (98b421f: producer queue, ingestor
  single producer and DOM-free routing, A1 lazy canonical JSON, B1 TCP_NODELAY, raw_age split), batch constant 1024
  (abfcc9f), consumer session timeout 10 s (f8e5442), same-consumer rewind on a rebalance abort with heartbeat 3 s
  (e19a9f6), typed rewind failure and epoch fence (RB1, 32fe603), native Kafka error classification (DL-R1, a4e7b3b).
- **Query**: G5 BAR warmup cold path renders validated rows directly and authorizes once per source (4d3476c,
  byte-identical output), typed `CANONICAL_LINEAGE_MISMATCH` (IDR-099, 00056e5), `retry_after_ms` 2000 on a full
  large-warmup queue (b7f80eb), Binance current OI entitlement and UTC-grid/settlement-grid coverage
  (IDR-090/091/092, 2524732).
- **Stream gateway** (N1-OPT and isolation): bounded replay batching and wake-ups (ed1b85b, aa1f98f), bounded
  near-frontier registration and catch-up (f0242dc, 3defbbb, 2743938), stalled-peer isolation and preemption
  (b3fdd43, c140e7e), copy-on-write subscriber lists (8ddba09), IDR-096 replay-budget status opt-in (f5cd787),
  LIVE carries the REPLAYING high watermark again (ffa382f), live-budget reserve for subscriptions at their floor
  (e3b5552), exact outstanding-record floor and attributable handoff refusals (Codex SR1/SR4, c934c1e), gateway
  dispatch/dequeue/send clocks (a388537).
- **Projector**: IDR-107 bounded retry of transient poll errors (58fb7cd) with an allowlist and commit-only budget
  reset (Codex CF3/CF4, ff5683c).
- **Kafka A' contract**: `md.latest.v2` carries no tombstones (bb15890: budget `state_topic_tombstones` v1, golden
  invariant, Rust guard test); tools for the cleaner I/O throttle (2eb77f8) and the marker retention (c767b91).
- **Apparatus and oracles** (scripts/tests only): G4 observer v2-v5, G9/G10/G11 native-writer oracles (Codex
  SR2/SR3/SR5), G12 sampler coverage, core-group harness (simultaneous replace), DR08 isolation campaign, G13 runner
  fixes.

## Public Interface

Every change is additive (owner rule 2026-10-07):
- `StreamRecord.gateway_dispatch_wall_ns = 3`, `gateway_dequeue_wall_ns = 4`, `gateway_send_wall_ns = 5`
  (diagnostics only; 0 = not measured).
- `SubscribeRequest.replay_budget_status_v2 = 8` (bool). Absent keeps the pre-2.4.1 `OUT_OF_RANGE CURSOR_EXPIRED`
  mapping; when set, replay work budgets end with retryable `RESOURCE_EXHAUSTED RATE_LIMITED:REPLAY_*_LIMIT`.
- No field number is reused or removed. Endpoint versions are unchanged.

SDK **2.4.1** (`<wheel sha256>`; preliminary host build at `aaa81a8`:
`9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7`, two identical builds), superseding 2.4.0 (vendored by TS, never published by a DL release): gateway
clocks on `StreamEvent` (optional, not part of equality), the field-8 opt-in, and reconnect budgets renewed only on
new progress. `generated_contract_digest` `2ee96b1edab01d3185e4fb973f7e3a0715956f8648938039108eb6156187c116` equals
the TS candidate pin `vendor/qdl_sdk-2.4.1-candidate-f5cd787.json`. A caller that uses `GrpcStreamTransport`
directly now sees `RESOURCE_EXHAUSTED` where 2.4.0 saw `OUT_OF_RANGE` for those budgets.

## Source And Images

- Release source: `<REV>` (the commit carrying the SDK 2.4.1 bump `aaa81a8`, the build recipe `46f4628` and this
  packet on `fix/p184-dl-closure`). Build: `python3 scripts/build_qdl_release_images.py --build --lock-owner <owner>`
  (dry-run without `--build`); receipt `~/.local/state/qdl-v2/build-v2.2.4-<sha7>/receipt.json`.
- Rust `qdl-v2-rust:2.2.4-<sha7>` `<digest>`: 12 binaries compiled at `<REV>` in builder `551428d9`, copied onto
  rc16 `f41ecb66` (the owner-approved thin method; no BuildKit Rust compile on the runtime disk).
- Python `qdl-v2-python:2.2.4-<sha7>` `<digest>`: recipe build of `Dockerfile` at `<REV>`.

| Role | Measured in stage-100 #3 | Release | Equivalence |
| --- | --- | --- | --- |
| rust_core x3 | rc22 `7c848aad` (a4e7b3b) | `<rust digest>` or kept (owner decision) | qdl-realtime-core sha256 `2d821673...` expected equal |
| stream_kn_1/2 | `baa72c96` (TEST overlay, gateway c934c1e, `qdl.test.only` label) | `<rust digest>` (must roll: no production role keeps a test label) | gateway sha256 `<...>` expected equal |
| market_projector_1/2 | rc14 `ce629061` | `<rust digest>` | different from production (IDR-107, CF3/CF4), expected equal to rc22 `efeb4728`: post-roll check |
| ingestor_okx_swap / binance_usdm | rc18 `7fba2577` | `<rust digest>` | different from production (qdl-kafka DL-R1/RB1), expected equal to rc22 `77ceeee1`: post-roll check |
| query_kn_1/2 | rc19 `b9ca6c36` (thin overlay) | `<python digest>` | /app files equal except the git-derived set (SDK files, query_pb2 field 8, budget config, `qdl/certification/phase3_consumer_load.py` apparatus) |
| binance_bar_edge | rc14 `08f356a8` | unchanged (HELD) | not on the release revision |

## Runtime Config

- Kafka log-cleaner I/O throttle: cluster dynamic default **4 MiB/s** (`log.cleaner.io.max.bytes.per.second=4194304`)
  since 2026-10-14 12:21:52Z. **Correction:** the v2.2.3 notes say 8 MiB/s. That was true when v2.2.3 was
  published (applied 2026-10-11T03:40:56Z) and is stale since 2026-10-14; the published v2.2.3 notes are not
  rewritten. Receipt for the 4 MiB/s value: `<describe output path + sha256>` (to be captured read-only).
- `md.latest.v2` `delete.retention.ms` 604800000 -> 21600000 (A', 2026-10-16T02:05:37Z, contract bb15890; rollback
  `--rollback-to 604800000 --apply`). Its effect arrives gradually (up to 7 d for markers already stamped).
- Kafka internal topics (decision **C**, owner "B + C 10 min", 2026-10-16): `__consumer_offsets`
  `delete.retention.ms=600000` + `segment.ms=60000`, `__transaction_state` `segment.ms=600000`, applied dynamically
  ~09:2xZ with `scripts/kafka_internal_topic_compaction.py --apply` (observe: APPLIED, no drift). Convergence is
  estimated at ~49 h because delete horizons already stamped at 24 h persist. Rollback: `--rollback --apply --cert-dir <stable cert dir>`
  (deletes the overrides). Receipt: the effective describe of the B final-check, 2026-10-16T14:28:49Z, effective
  sha256 `f4836dfc9c9f09a72882bcdb6fcc557b678bfc29738426854f09dd038b78f8c4`
  (`~/.local/state/qdl-v2/p184-kafka-b-20261016/final-check-20261016T142831Z/production-parity.json`). It shows C,
  A' and rebalance false on all three brokers, with no temporary key.
- Temporary `min.cleanable.dirty.ratio=0.05` on `__consumer_offsets` only (owner-approved): applied 12:54:49Z and
  removed 14:12Z. Repeated cleaner passes shrank `__consumer_offsets-43` from ~1.65 GB / 18.4M records to
  ~230-260 MB / ~2.95-3.2M records on all three replicas; the size plateaued from ~13:50Z. Group offsets before and
  after: 5/5 groups, none missing or decreased. A DeleteRecords force-trim is impossible, because Kafka 4.2 refuses
  it on internal topics.
- Kafka `auto.leader.rebalance.enable=false` (decision **B**): **applied 2026-10-16** by sequential recreate:
  kafka1 14:14Z, kafka2 14:21Z, kafka3 14:25Z (quorum leader and core-group coordinator, last).
  - Each roll passed a manual HOLD gate (Codex F1): broker healthy in ~20 s, ISR in ~3.7-3.9 s, no other role
    restarted, writer demanded == session-ready in every sample, 0 eligibility losses, READY 64/64 between steps.
  - Core commit stall max per roll: 2.5 s / 3.3 s / 5.2 s.
  - A data-scope preferred election followed; the final check found env false x3.
  - Receipts: `~/.local/state/qdl-v2/p184-kafka-b-20261016/`.
  - Rollback, one broker at a time: `scripts/kafka_auto_leader_rebalance_roll.py --rollback <packet> --broker kafkaN --apply`.
- Kafka CPU: kafka1 1.25 -> 1.75 vCPU (2026-10-14 13:47:40Z, docker update); kafka2/3 already 1.75.
- Core config `releases/v2.2.3-rc18-core-batch1024` (batch 1024), ingestor_binance_usdm 0.75 vCPU (2026-10-15).

## Findings Since The Preparation Packet

- **Coordinator load (G12 broker-restart mechanism).** Production `__consumer_offsets-43` (the core group's
  coordinator partition) held 1.65 GB: 14.31M transaction markers and 4.07M offset-commit records (read-only dump).
  A coordinator move replays the whole partition, and the core's transactional commit waits for it. Harness fit:
  ~8.0 us per offset-commit record + ~0.33 us per marker, so a move today is projected at ~37.5 s unloaded
  (inferred, not measured in production). Decision C bounds the marker lifetime; B removes the automatic moves.
  Evidence `~/.local/state/qdl-v2/p184-g12-coordinator-load-20261016/`.
- **First real production coordinator load (2026-10-16 14:25Z, kafka3 roll, loaded on broker 2).**
  `__consumer_offsets-43` loaded 3,201,999 records / 267 MB in **4,783 ms**. Of that, 1,593 ms was spent queued
  behind `__consumer_offsets-24`, which loaded 1.88M records in 1,592 ms. This was measured after the temporary
  cleaner pass, so it replaces the ~37.5 s projection, which was for the 18.4M-record partition. A coordinator move
  costs ~5 s today and falls as C converges. Source:
  `p184-kafka-b-20261016/roll-kafka3-20261016T142516Z/coordinator-loads.json`.
- **IDR-108 (OPEN, TS `ba31e808`).** `stream_kn` accepts on 8210 before its hot view serves. A client that connects
  right after a Stream restart can get `DATA_NOT_READY` until the hot view serves. In the TEST_ONLY measurement, all
  10 bindings served at port-ready +356.6 ms, and one QUOTE `DATA_NOT_READY` came at +39.7 ms. The v2.2.4 source does not change this, so it is a stated limit unless fixed before the build.
- **TS writer.** Production writer B16 `d27db0cf` was replaced by B17 `cb5216b8` (TS `68f2572f`: QUOTE OBSERVE probe
  at bound/3 for quota margin, liveness diagnostics, native probe observability) at 2026-10-16T09:27:01Z. Rollback B16.
  The writer is a TS release item; it is listed here because the certification run measures the DL with it.
  The **final** writer is not B17. It is the canonical image from the final TS commit, which adds 1a648ea8: an
  uncharged replay continuation only on verified durable progress. The reviewed delta and the proof map are in TS
  `upgrade/evidence/P184_WRITER_RELEASE_PACKET_20261016.md` section 8 (TS `8b497acf`).
- **Harness parity.** The isolated TEST harness capped its Redis at 0.5 CPU while production `redis_marketdata` is
  unlimited. Coordinator decision: isolated harness Redis 1.0 CPU for capacity cells; frozen rules unchanged.

## TEST_ONLY Burst Results (not release gates)

Internal capacity on production image pins in isolated namespaces (raw publish to Redis ACK and cursor; not
provider, not venue latency, fresh brokers):
- Ladder on B16 (prereg TS `3d9582de`): 7/8 CLEAN; `b16-9k-2` NOT_CLEAN on rule S (one overflow).
- Amendment-1 (writer Subscribe depth 2000): 4/5 CLEAN; `a1-corepause9k` NOT_CLEAN on rule S (overflow ~0.8 s after
  a 780 ms core pause) while exactness and recovery passed.
- Drain-rate diagnosis: the binding stage was the harness Redis CPU cap (rows16: ~11.3k/s at 0.5 CPU, ~13.3k/s at
  1.0 CPU).
- Parity rerun of the 5 amendment-1 cells with the harness-only Redis CPU cap removed (tsredis 1.0):
  - 4/5 CLEAN, including rule S: 9k x2, writer pause, and core pause 700 ms.
  - `a1-streamrs9k` NOT_CLEAN: `DATA_NOT_READY_AFTER_UPSTREAM_READY`, caught up 22.2 s.
- Stream-serving addendum (owner-approved, frozen before data): `a1s-streamrs9k` CLEAN.
  - First apply +175 ms, caught up +9.9 s after port-ready.
  - Root cause: IDR-108.
- **Closure.** On the Redis-cap-removed profile with writer B16, these cells are CLEAN: 9k no-fault x2, writer
  pause, core pause 700 ms and stream restart. Every earlier NOT_CLEAN receipt is kept.
- Replay pages after an overflow are now classified per episode (Codex R2). TS 1a648ea8 grants an uncharged replay
  continuation only on verified durable progress.

## Proof Map Per Component (Codex principle E)

Every row needs the build post-check before it can be claimed. "Identical" is an expectation from the source delta
until the post-check shows the bytes. Machine form: `proof_map` in [release-preparation.json](release-preparation.json).

| Component | Release binary vs production | Affected behaviour | Retained receipt | Remaining step |
| --- | --- | --- | --- | --- |
| rust_core x3 | identical expected (`qdl-realtime-core` == rc22 `2d821673...`, no source delta since a4e7b3b) | none if EQUAL | stage-100 #3 (diagnostic); core-group S2 `4626b44a` (INHERIT); B-roll core progress | post-check EQUAL, else stop; owner B5: keep rc22 containers (recommended) or simultaneous replace |
| stream gateway (stream_kn_1/2) | gateway identical expected to the one in `baa72c96`; the image differs (test overlay, `qdl.test.only`) | none in the gateway if EQUAL | stage-100 #3 on `baa72c96`; isolated SR1 A/B; TEST_ONLY stream-serving CLEAN | **rebuild as the release image and roll** stream_kn_1 then 2 (writer READY 64/64 after each); IDR-108 stated |
| ingestors | differs (rc18 `8393bb1f` -> rc22 `77ceeee1` expected; qdl-kafka DL-R1/RB1) | native Kafka error classification at the supervisor | rc22 post-check; stage-100 #3 measured rc18 | roll one at a time with a 5 min watch; measured by the final stage-100 |
| projectors | differs (rc14 `ce629061` -> rc22 `efeb4728` expected; IDR-107, CF3/CF4) | bounded retry of transient poll errors on broker stop or coordinator move | TEST projector `381d6bf9` broker9k (TS N1 r2d2); G12 projector restart only on rc14 | roll 1 then 2 with a post-roll check; IDR-107 deployed after; broker-stop coverage per FINAL_STEPS Q1 |
| query | python files equal except the git-derived set; venv and OS layer rebuilt | none on the Query import path | stage-100 #3 on rc19; G13 DR01/DR07 INHERIT candidates | file verdict + pip freeze == rc19; roll 1 then 2; G13 runtime on the release digest |
| Kafka config | no image change (`9516fb76`) | coordinator moves (B), marker lifetime (C, A'), cleaner duty | effective describe `f4836dfc...`; B receipts; coordinator load 4,783 ms | G12 final credit binding the effective config (Codex K3); cleaner 4 MiB/s describe (B9) |
| SDK 2.4.1 wheel | host build `9f79e032...` (preliminary) | additive wire, reconnect budget | host build x2 | two `--network none` builds at `<REV>`; CI `sdk-python310` |
| binance_bar_edge | unchanged rc14 `08f356a8` (HELD) | n/a | n/a | scoped out |

## Gate Scorecard (G1-G14)

`<from the post-rollout certification run on the release digests and the G12/G13 receipts (stage-100 #3,
~/.local/state/qdl-v2/p184-final-campaign-20261016T024530Z, is diagnostic only: protocol_met false); no frozen gate
widened; a NOT_MEASURED is never upgraded>`

## Limits

- Single host for every run: no HA, no independent failure domains, no mainnet or funded claim.
- binance_bar_edge is not on the release revision.
- Kafka C converging at measurement time (a coordinator move cost ~5 s on 2026-10-16, decreasing); B applied
  2026-10-16 14:25Z.
- IDR-108: after a restart, Stream readiness (port accept) comes before hot-view serving. TEST_ONLY measurement:
  serving at port-ready +356.6 ms.
- TEST_ONLY burst results above are not production capacity claims.
- `<roles certified by binary/file equality rather than their own measurement>`
- `<G12/G13 inheritance basis>`

## Rollback

Per role, failure only, one container at a time with the saved composes: query `b9ca6c36`, stream `baa72c96`,
projector `ce629061`, ingestor `7fba2577`, core `7c848aad`. Kafka config: cleaner `--add-config
log.cleaner.io.max.bytes.per.second=8388608`; marker retention `--rollback-to 604800000 --apply`; internal topics
`scripts/kafka_internal_topic_compaction.py --rollback --apply --cert-dir <stable cert dir>`.

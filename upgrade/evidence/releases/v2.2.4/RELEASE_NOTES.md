# Quant Data Layer v2.2.4 (DRAFT)

Status: **DRAFT - not certified, not published.** This file is the release-notes skeleton written during the
stage-100 #3 measurement window (host lock `claude-n1-opt-20261013`, 2026-10-16). It holds no gate result. Every
`<...>` placeholder is filled from receipts after stage-100 #3, the release image build and the equivalence check.
The full inventory, build plan and open items are in [release-preparation.json](release-preparation.json).

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

SDK **2.4.1** (`<wheel sha256>`), superseding 2.4.0 (vendored by TS, never published by a DL release): gateway
clocks on `StreamEvent` (optional, not part of equality), the field-8 opt-in, and reconnect budgets renewed only on
new progress. `generated_contract_digest` `2ee96b1edab01d3185e4fb973f7e3a0715956f8648938039108eb6156187c116` equals
the TS candidate pin `vendor/qdl_sdk-2.4.1-candidate-f5cd787.json`. A caller that uses `GrpcStreamTransport`
directly now sees `RESOURCE_EXHAUSTED` where 2.4.0 saw `OUT_OF_RANGE` for those budgets.

## Source And Images

- Release source: `<REV>` (the SDK 2.4.1 version-bump commit on `fix/p184-dl-closure`).
- Rust `qdl-v2-rust:2.2.4-<sha7>` `<digest>`: 12 binaries compiled at `<REV>` in builder `551428d9`, copied onto
  rc16 `f41ecb66` (the owner-approved thin method; no BuildKit Rust compile on the runtime disk).
- Python `qdl-v2-python:2.2.4-<sha7>` `<digest>`: recipe build of `Dockerfile` at `<REV>`.

| Role | Measured in stage-100 #3 | Release | Equivalence |
| --- | --- | --- | --- |
| rust_core x3 | rc22 `7c848aad` (a4e7b3b) | `<rust digest>` or kept (owner decision) | qdl-realtime-core sha256 `2d821673...` expected equal |
| stream_kn_1/2 | `baa72c96` (TEST overlay, gateway c934c1e) | `<rust digest>` | gateway sha256 `<...>` expected equal |
| market_projector_1/2 | rc14 `ce629061` | `<rust digest>` | different (IDR-107, CF3/CF4): post-roll check |
| ingestor_okx_swap / binance_usdm | rc18 `7fba2577` | `<rust digest>` | different (qdl-kafka DL-R1/RB1): post-roll check |
| query_kn_1/2 | rc19 `b9ca6c36` (thin overlay) | `<python digest>` | qdl/ + app/ files expected equal |
| binance_bar_edge | rc14 `08f356a8` | unchanged (HELD) | not on the release revision |

## Runtime Config

- Kafka log-cleaner I/O throttle: cluster dynamic default **4 MiB/s** (`log.cleaner.io.max.bytes.per.second=4194304`)
  since 2026-10-14 12:21:52Z. **Correction:** the v2.2.3 notes say 8 MiB/s. That was true when v2.2.3 was
  published (applied 2026-10-11T03:40:56Z) and is stale since 2026-10-14; the published v2.2.3 notes are not
  rewritten. Receipt for the 4 MiB/s value: `<describe output path + sha256>` (to be captured read-only).
- `md.latest.v2` `delete.retention.ms` 604800000 -> 21600000 (A', 2026-10-16T02:05:37Z, contract bb15890; rollback
  `--rollback-to 604800000 --apply`). Its effect arrives gradually (up to 7 d for markers already stamped).
- Kafka CPU: kafka1 1.25 -> 1.75 vCPU (2026-10-14 13:47:40Z, docker update); kafka2/3 already 1.75.
- Core config `releases/v2.2.3-rc18-core-batch1024` (batch 1024), ingestor_binance_usdm 0.75 vCPU (2026-10-15).

## Gate Scorecard (G1-G14)

`<from ~/.local/state/qdl-v2/p184-final-campaign-20261016T024530Z and the G12/G13 receipts; no frozen gate widened;
a NOT_MEASURED is never upgraded>`

## Limits

- Single host for every run: no HA, no independent failure domains, no mainnet or funded claim.
- binance_bar_edge is not on the release revision.
- `<roles certified by binary/file equality rather than their own measurement>`
- `<G12/G13 inheritance basis>`

## Rollback

Per role, failure only, one container at a time with the saved composes: query `b9ca6c36`, stream `baa72c96`,
projector `ce629061`, ingestor `7fba2577`, core `7c848aad`. Kafka config: cleaner `--add-config
log.cleaner.io.max.bytes.per.second=8388608`; marker retention `--rollback-to 604800000 --apply`.

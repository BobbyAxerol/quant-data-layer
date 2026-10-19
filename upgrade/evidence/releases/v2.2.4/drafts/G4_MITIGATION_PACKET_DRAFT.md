# DRAFT owner packet: targeted G4 mitigation (Kafka commit tail)

Status: DRAFT, 2026-10-17. Use ONLY IF the clean-host final stage-100
(`/home/bobby/.local/state/qdl-v2/p184-final-campaign-20261017T133644Z`) still fails G4 on a
clean (untainted) run. If that run passes G4, discard this packet. Nothing here is approved, applied or
measured on a changed system. Thresholds, windows and run timing are not touched by this packet.

## 1. Evidence base (read-only RCA, plan entry "2026-10-17 ~11:35-11:55Z RCA", DL plan commit bafbe91)

- G4 failing rows of the 105025Z run: `bar|BINANCE|1m` p95 270.7 ms (n=15, so p95 = max; SOLUSDT 1m close
  11:00:00Z) and `book_delta` / `quote` `|OKX_INVERSE` max (321.8 / 359.5 ms, inside checkpoint
  10:56:54-10:57:24Z together with every linear row of both venues).
- Mechanism (correlation verified, causation inferred): common-mode Kafka transactional commit stalls on all three
  rust_core, `commit_ms` max 154 / 162 / 176 ms at 10:57:25Z; in w3 the `md.latest.v2-3` log-cleaner offset-map
  phase ran 10:59:41-11:02:21Z on all three brokers, with broker cgroup throttling 0.6-3.0 s per 12.5 s
  (about 5-24 % of periods) at the 1.75 vCPU quota, on top of the 11:00Z boundary burst. w5 overlapped the
  `md.latest.v2-2` phase without a boundary and passed.
- Contributor, smaller: young GC 10-26 ms at ~0.7/s on the brokers (single pauses max 63 ms were excluded as a
  sole cause).
- Same row set and same common-mode stall also at 032230Z with no md.latest cleaner phase and light foreign reads,
  so the cleaner is a sufficient aggravator, not the only one (thin margin at boundary windows).
- Excluded: ingestor_okx_swap throttling (0 ms), venue/WS, observer, G11 restart, trace re-arm.
- Not yet known (to be measured before choosing): whether the clean run's tail rows again coincide with a cleaner
  phase. The guard's report-only samplers (core `commit_ms`, per-broker cleaner phase) give this for free:
  `<campaign>.guard/`. If the failing rows do NOT coincide with a cleaner phase, candidate 1 is not justified and
  this packet must be re-ranked (E1/E2: no fix before the cause is shown on the clean run).

## 2. Current Kafka facts (all read from files, nothing run)

| Item | Value | Source |
|---|---|---|
| Broker quota / heap | live 1.75 vCPU / 2 GiB per broker (repo compose still shows 1.25; release draft raises to 1.75), `-Xms512m -Xmx768m`, G1, `MaxGCPauseMillis=100` | `docker-compose.v2-stable.yml:58-59,184-198`; `drafts/compose-v2-stable-release-draft.diff` |
| Cleaner threads / dedupe buffer | not set in compose, so Kafka defaults (1 thread; 128 MiB buffer, to be confirmed with `kafka-configs --describe --all` before use) | grep of `docker-compose.v2-stable.yml`: no `log.cleaner.*` |
| Cleaner I/O throttle | cluster dynamic default `log.cleaner.io.max.bytes.per.second` = 4 MiB/s (set 10-14; 8 MiB/s on 10-11) | plan lines 72165, 73606; `scripts/kafka_cleaner_io_throttle.py` |
| A' | `md.latest.v2` `delete.retention.ms` 604800000 -> 21600000 (6 h), applied 10-16 02:05:37Z | plan 10-16 entry; `scripts/kafka_md_latest_marker_retention.py` |
| C | `__consumer_offsets` `delete.retention.ms=600000`, `segment.ms=60000`; `__transaction_state` `segment.ms=600000`; `__consumer_offsets` `min.cleanable.dirty.ratio` 0.05 temporary (owner 10-16) | plan 73663; memory `kafka-b-now-not-wait-convergence` |
| B | `auto.leader.rebalance.enable=false` on kafka1-3 | plan 10-16 entry |
| Shared cleaner | one cleaner thread serves `md.latest.v2` and the internal topics | plan 73669 |
| Budget | DL average 5.57 vCPU in the FINAL 105025Z run (G14); owner budget <= 5.0 vCPU standing rule, capacity package ceiling 6.5 vCPU, no extra RAM without owner approval | plan 74356; memory `capacity-package-rust-first-20261012` |

## 3. Candidates, one variable each

### C1 (RECOMMENDED FIRST): lower cleaner I/O throttle 4 -> 3 MiB/s (cluster dynamic default)

- Variable: `log.cleaner.io.max.bytes.per.second` only, via the existing tool
  `scripts/kafka_cleaner_io_throttle.py` (dry-run default, refuses under a foreign host-measurement lock, refuses
  below the keep-up floor, `--rollback-to` / `--delete-config`).
- Mechanism targeted: the cleaner's read/write burst competes with the broker request threads for the 1.75 vCPU
  cgroup; a lower rate lowers the cleaner's CPU duty per second and therefore the throttled-period fraction during an
  offset-map / copy phase. Honest limit (inferred, not measured): the phase gets longer (about 4/3), so the exposure
  window grows while intensity falls; the benefit exists only if the commit tail is driven by intensity, not by
  presence. That is exactly what the A/B decides.
- Keep-up check from the tool's own arithmetic (measured 0.49 processed per throttled byte; demand 0.89 + 0.13 =
  1.02 MB/s per broker; duty cap 0.8): 4 MiB/s gives capacity 1.96 MB/s (duty 0.52); 3 MiB/s gives 1.47 MB/s
  (duty 0.69, passes); the floor is about 2.6 MiB/s, so 3 MiB/s is the lowest sensible step and 2.5 MiB/s would
  be refused by the tool. Previous note: B (3 MiB/s) was "not taken alone (small benefit, untested)" in the 10-15
  decision; this packet is the test.
- Expected effect: moderate/uncertain on the G4 rows (reduces cleaner share of broker CPU by up to ~25 % of its
  own use); no effect on the boundary burst itself or on young GC.
- Resource delta: zero CPU quota, zero RAM; disk use of `md.latest.v2` dirty tail may rise slightly (cleaner lags
  a bit); watch partition sizes of `md.latest.v2` and `__consumer_offsets-43`.
- Measurement plan:
  1. Isolated A/B first on the G12/N1 harness (fresh Kafka with production parity: C, A', B; the harness already
     has a production-parity check) with a `md.latest.v2` aged dirty state and production-like core config and
     sustained load. Arms: 4 MiB/s vs 3 MiB/s, same seed, at least 3 boundary windows per arm. Target metrics:
     core `commit_ms` max and p99 per window, broker cgroup throttled-period fraction per 12.5 s, cleaner phase
     duration; scored rows are the G4 rows themselves. Proxy metrics only diagnose (one-variable rule).
  2. Production only if the A/B shows a lower `commit_ms` tail with a cleaner phase duration that still keeps up
     (no growing dirty tail). Apply with the tool, HOLD gate: a clean-host stage-100 #4 after the change; no other
     variable in flight; no revert while the cleaner backlog drains.
- Blast radius: cluster-wide dynamic broker default, no broker restart, no consumer-group change, no topic
  config touched, `delete.retention.ms` untouched.
- Rollback: `scripts/kafka_cleaner_io_throttle.py --rollback-to 4194304` (value read before) or `--delete-config`
  if the key had no default; effect immediate. Cleanup: remove the throwaway admin container (`--rm`).
- Approval needed: owner (production Kafka config change); isolated A/B does not need production approval.

### C2: Kafka broker CPU quota 1.75 -> 2.0 vCPU (only after C1 is measured)

- Variable: broker `cpus` (docker update, no restart, the same path used for 1.25 -> 1.75).
- Mechanism: directly removes cgroup throttling (0.6-3.0 s per 12.5 s) during cleaner + boundary overlap, the most
  direct cause in the RCA.
- Resource delta: +0.25 x 3 = +0.75 vCPU ceiling. DL average is already 5.57 vCPU against the 5.0 rule and the 6.5
  capacity ceiling, so this needs explicit owner resource approval with numbers, and only after C1 is shown
  insufficient (rule: optimise first, then ask). Reclaim plan: the same step must offer an equal cut elsewhere or
  state the net sum against 6.5.
- Measurement: same A/B harness with quota as the single variable, then production HOLD with a clean stage-100.
- Rollback: `docker update --cpus 1.75` on each broker, no restart.
- Risk: raising a producer-side/central role on an oversubscribed host (16 vCPU shared with TS, alphas, portal)
  can move the bottleneck; watch host CPU PSI and venue-to-durable p50/p90 between rounds.

### C3: core transaction batching (fewer, larger commits)

- Variable: rust_core commit cadence / batch (product change). Reduces the number of commits exposed to a stall but
  lengthens in-flight transactions, raising the per-commit blast of one stall and lag; current production batch is
  1024 with transactions already in flight.
- This is a consumer-group-adjacent change: per the 10-15 incident rule, it needs a multi-member reproduction
  receipt (3 cores, production-like config, sustained load, simultaneous replace of members, no mixed-version
  roll) before production. Needs a new core image and recreate of all three cores: largest blast radius, slowest
  to prove. Not first. Resource delta: none expected.
- Rollback: restore the last stable core image set from the rollout tool's saved composes, verify membership
  and 0 retries.

### C4: Kafka heap / GC tuning (RAM owner-approved)

- Variable: `KAFKA_HEAP_OPTS` and/or G1 pause target. Mechanism: young GC 10-26 ms at 0.7/s is a minor
  contributor (single pauses max 63 ms did not match the tails). Expected effect on G4: low.
- Needs a broker restart (rolling kafka1 -> 2 -> 3, quorum leader last, declared maintenance interruption like B)
  and extra RAM only after owner approval. Not recommended before C1/C2.

## 4. Ranking and recommendation

1. C1 (3 MiB/s cleaner throttle): cheapest, no restart, no resource delta, tool and rollback already exist, targets
   the cleaner mechanism that overlapped the failing window. Do the isolated A/B first.
2. C2 (quota 2.0): most direct on throttling but costs +0.75 vCPU against an already over-budget average; owner
   resource request only after C1.
3. C3 (batching): highest blast radius and proof cost.
4. C4 (heap/GC): smallest expected effect, needs restarts.

Recommended first step if the clean run still fails G4 AND its guard samplers show the failing rows coincide with
a cleaner phase and broker throttling: approve the isolated 4 vs 3 MiB/s A/B on the N1/G12 harness (no
production change); decide on a production apply of C1 from its receipt. If the clean-run failure shows no cleaner
phase, return to RCA before choosing.

## 5. Not in scope

No change to G4 thresholds, windows or scoring, no change to run timing, no change to the frozen tool / observer /
budget. Host-measurement lock applies to the A/B and to any production apply.

## 6. Open items before sending to the owner

- Confirm the live dynamic value 4 MiB/s and the cleaner thread / buffer defaults with a read-only
  `kafka-configs --describe` (not run: host is inside a measurement window).
- Fill in the clean-run G4 table and guard cleaner-phase timeline.

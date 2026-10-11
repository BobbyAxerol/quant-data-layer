# IDR-062 Offline Paired Measurement

Status: PAIRED_OFFLINE_MEASURED_NOT_RUNTIME_CERTIFIED, 2026-10-07.
No shared-runtime mutation, image build, TS edit, push or merge.

## Provenance and method

- Existing baseline: `sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023`.
- Existing rc2-43bbcf0: `sha256:69e3b2b214331a7ebb6a110dd8159c899bbbd6439aaf87293c8c68d145e3713d`.
- Public capture begins 2026-10-07 12:52:17.878 UTC, approximately 60 s per venue:
  ETHUSDT Binance USD-M (592 frames) and ETH-USDT-SWAP OKX (601 frames).
  Bindings copied read-only from the running core, not a reference-symbol guess.
- Capture SHA256: `3afa271cb5f1e99c84ebb68a90bdd1719b740c39c1bca7826f53c4727868d4a9`.
- Both core configs SHA256: `54c0fece8e9535113d8960ede65bedaf7c1d9f02d6c470387d07b2f667d34cbc`.
- Same capture, pacing, config, TLS, partition count and 1 CPU / 256 MiB limit;
  only core image changes. Each run uses a fresh disposable Kafka broker on an
  internal test network (no host ports, production secrets or shared state).
  Core is the unmodified executable packaged in each image, not a Python model.
- Capture API: `scripts/phase114_l2_real_provider_capture.py:capture_replay_window`;
  orchestration/oracle: `scripts/measure_l2_top_materialization.py`.
- Raw capture/output files deleted after hashing. Compact evidence retained at
  `/home/bobby/.local/state/qdl-v2/idr062-paired-20261007/run5/`.
  `paired-receipt.json` SHA256:
  `b9d14de49256e7fb15b80faa430c52a610f18d5aa83c1be6dfc88f85e4de4ecc`.

## Snapshot output

Includes initial bootstrap snapshots. Rates divide by the actual provider window,
60.008 s Binance / 60.000 s OKX. Bytes are serialized EventEnvelope protobuf bytes,
NOT Kafka framing, TLS, compressed on-disk size or raw provider payload bytes.

| Venue | Baseline snapshots n | Candidate n | msg/s baseline -> candidate | bytes/s baseline -> candidate |
| --- | ---: | ---: | ---: | ---: |
| Binance ETH | 59 | 87 | 0.9832 -> 1.4498 | 7807.9 -> 11509.5 |
| OKX ETH | 61 | 87 | 1.0167 -> 1.4500 | 8329.5 -> 11877.4 |

587 Binance and 600 OKX delta payloads have equal sequence-ordered SHA256 chains
between cores. Snapshot top at each source sequence matches an exact Decimal
provider-book oracle. No post-bootstrap gap/refusal/quarantine; each run records
one expected Binance `L2_RESNAPSHOT_REQUIRED_V1` before initial snapshot readiness.
That advisory is counted, not hidden. Four pre-bootstrap frames are buffered, not
lost. No duplicate canonical payload claim is inferred from container health.

## Top-change delay

Time from provider top-change event to first verified snapshot at the same or a
later source sequence, in **provider event-time ms**. A later sequence may coalesce
intermediate tops. This isolates publication-policy delay. It is NOT request RTT,
normalization CPU time, Kafka commit latency or consumer usable latency. Zero means
snapshot emitted on the same event, not zero wall-clock processing time.

| Venue / image | p50 ms | p95 ms | max ms | matched n | right-censored tail |
| --- | ---: | ---: | ---: | ---: | ---: |
| Binance baseline | 510 | 922 | 924 | 43 | 1 |
| Binance candidate | 0 | 0 | 0 | 44 | 0 |
| OKX baseline | 500 | 900 | 900 | 41 | 0 |
| OKX candidate | 0 | 0 | 0 | 41 | 0 |

Activity: 44 Binance and 41 OKX top changes, so this is not a motionless-book probe.
It is one short public-market window, NOT a high-volatility/peak-load certificate.
Only initial bootstrap is captured; periodic REST renewal, disconnect and resync
faults are not replayed in this measurement (unchanged source tests are inherited).
For the stricter metric requiring that exact top before it is superseded:
Binance baseline p50/p95/max 408/922/924 ms, n=24, 19 superseded, 1 censored;
OKX baseline 300/900/900 ms, n=26, 15 superseded, 0 censored.
Candidate: 0/0/0 ms, n=44 and 41 respectively, none superseded/censored.
No p99 is claimed from these small denominators; quantiles use nearest rank.
A test prevents a later keepalive snapshot with the same sequence from being
backdated onto an earlier update.

## Core resources and downstream estimates

Two-binding replay through one core, not three production cores or the entire DL.
Process CPU is sampled from /proc user+system ticks (100 ticks/s); RSS is maximum
observed VmHWM. 101 samples per image, target cadence 0.5 s plus probe overhead;
observed spans 60.642 / 60.835 s. The last process sample can precede exit by one
sample interval. Startup is included; no confidence interval from one pair.

| Core image | observed CPU s | CPU s / 60 s (vCPU) | observed RSS HWM KiB |
| --- | ---: | ---: | ---: |
| Baseline | 3.60 | 0.0600 | 19324 |
| Candidate | 3.76 | 0.0627 | 19612 |

Observed process CPU +4.44%, RSS +288 KiB. Container CPU including docker-exec
instrumentation is separately recorded (7.588 / 7.770 s), NOT passed off as core
CPU. Neither measured container was throttled in sampled cgroup counters.

**Estimates only**, at unchanged workload/fan-out and payload mix:
- Snapshot-only publication work: Binance x1.475, OKX x1.426, combined x1.450.
- Total BOOK message work (snapshot + unchanged delta): Binance x1.0433,
  OKX x1.0393, combined 1307 -> 1361 messages, x1.0413. This is the relevant
  message-volume estimate for a projector processing both types.
- Stream snapshot-only subscribers see the snapshot multiplier; delta-only
  subscribers have unchanged counts. Mixed subscription work depends on fan-out.
- Query RPC demand does not increase merely because core publishes more snapshots.
  Cache write/materialization work increases; Query CPU/latency cannot be derived
  by multiplying snapshot count. No measured projector/Query/Stream CPU is claimed.
- No extrapolation to all bindings, 5 vCPU whole-stack PASS, 20-50 alpha capacity,
  TS coherence or consumer latency. Those require the held post-core runtime gate.

## Verification, failed harness attempts and cleanup

17 unittest cases PASS (9 measurement + 8 existing capture), zero fail/skip on the
final run using the existing Python candidate image. Tests cover censored changes,
sequence/top mismatch, keepalive timing, capture bounds, bootstrap-only advisory,
provider continuity and exact test-container auto-remove handling.
Earlier attempts are NOT accepted measurements: run1 invalid 32-byte capture ID
(correct contract is 16) plus client name race; run2 rejected the expected bootstrap
advisory; run3 used the wrong numeric quarantine enum (now imports the protobuf
constant); run4 had valid baseline but failed candidate start on broker auto-remove
race. The final harness waits for auto-remove with a deadline; no production code
or provider rules were weakened. Those runs' compact logs/digests remain.

All test containers and the internal network removed; all five runs' raw captures,
canonical payload dumps and fresh private test keys removed. No images built or
removed: active/candidate/named rollback digests retained, shared BuildKit cache
not pruned. Docker inventory: 92 images / 28.19 GB; cache 227 entries / 16.83 GB.
Disk free for final run: 100669689856 -> 100420591616 bytes. Concurrent host writes
mean this decrease is NOT test residue attribution or a claimed reclaimed amount.
All inventoried DL image/start/restart fields unchanged. Canonical remains
`/home/bobby/data_layer main@5bf5cd5`, owner plan hunk untouched; feature worktree
`/home/bobby/.worktrees/data-layer-p184-closure`, `fix/p184-dl-closure` retained.
Published release remains v2.2.2. Runtime roles/digests/config fingerprints and
rollback set remain those in the Phase C packet; no binding V3 was mounted.

Next: review this result and the amended core-first Phase C packet. STOP; no
runtime deployment authority is inferred from this offline result.

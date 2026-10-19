# OPT resource package (round 2): CPU/RAM for the roles behind the G2/G4 tails

Status: **PACKET, not applied.** Written 2026-10-18 ~16:00-16:40Z from read-only evidence; no runtime change, no
container started by this work, no Kafka config altered.

Owner decision, 2026-10-18 ~16:00Z, verbatim: "Nâng rust kèm gói tối ưu CPU, Ram lần 2 gate có thể fail lúc trước,
tối ưu hết về phía mình xong, nếu gate không đạt xác định là lỗi external thì nới gate rồi release gọn nhé. Không cần
chờ nữa". The owner approves CPU/RAM increases for the DL roles that drive the G2/G4 tails, bundled with the rustls
rust-image roll and measured once, in a final stage-100.

Scope: config/resource only. `docker update` of `cpus` / `memory` on live containers, followed by a compose record of
the new values. There is no product code change and no TS writer change, because a writer change would void the G12
and G13 credits. No image, env, mount, topic config or broker config is changed. The rust-image roll has its own
packet and is not specified here; section 8 says how the two interact.

Labels: **[verified]** means read from a file or recomputed by a command listed here. **[inferred]** means reasoned
but not proven.

## 1. Proposal at a glance

| Role | Current (docker inspect 2026-10-18 ~15:55Z) | Proposed | Change type | Tail targeted |
| --- | --- | --- | --- | --- |
| `kafka1`, `kafka2`, `kafka3` | 1.75 vCPU each (mem 1.5 / 2 / 1.5 GiB, heap `-Xms512m -Xmx768m`) | **2.25 vCPU each**; mem and heap unchanged | `docker update --cpus`, no restart | G4 inverse, segment CreateTime -> observer (core transactional commit) |
| `query_kn_1`, `query_kn_2` | 1.5 vCPU / 1.5 GiB each | **2.0 vCPU / 2 GiB each** (`--memory-swap 4g`, the compose 2x ratio) | `docker update`, no restart | G2 MARK BINANCE via Query; G14 / OOM headroom of `query_kn_1` |
| `ingestor_okx_swap` | 0.5 vCPU / 256 MiB | **1.0 vCPU**; mem unchanged | `docker update --cpus`, no restart | G4 inverse, segment receive -> CreateTime |
| everything else | unchanged | unchanged | none | see section 5 |
| cleaner I/O throttle (C1) | 4 MiB/s (cluster dynamic default) | **unchanged** (C1 kept out of the bundle) | none | section 5.1 |

Sums. The DL CPU ceiling sum goes from 17.25 to 20.25 vCPU (+3.0). The DL memory limit sum goes from 21,408 to
22,432 MiB (+1,024; query only). Expected mean draw: +~0.11 vCPU (section 6).

## 2. Evidence base

The counted run is `p184-final-campaign-20261018T123222Z`: load 12:34:45-13:04:45Z, guard CLEAN.

| File | sha256 (prefix) | Used for |
| --- | --- | --- |
| `.../p184-final-campaign-20261018T123222Z/stage100/host.json` | `97e1a89372e5a4a5` | per-container cgroup `cpu.stat` every ~12.6 s (143 intervals in the load) |
| `.../stage100/receipt.json` | `3fd22d861ae434c7` | driver READ outliers, worker CPU |
| `.../scorecard.json` | `b9e76a83795f1af8` | G2 / G4 / G14 measured values |
| `.../a3-local/local.jsonl` | `9c3c4756f6a708a4` | ingestor cgroup and run-queue every 1 s |
| `....guard/cores.jsonl` | `4e582e48009dd218` | rust_core `commit_ms` per 100-batch window |
| `....guard/cleaner.jsonl` | `852cd68d9b15ae4f` | log-cleaner phases per broker |
| `....guard/host.jsonl` | `3d8bf4c4efd95932` | host load1 |
| broker G1 logs (`docker exec ... cat /opt/kafka/logs/kafkaServer-gc.log*`, read-only, extracted 2026-10-18 ~16:04Z) | scratch `gc-kafka{1,2,3}.txt` `4abc1c2a` / `f702a5dc` / `b4c505d9` | GC pauses in the load |
| Query `qdl.stall` lines (`docker logs --since 12:34:45Z --until 13:04:45Z`, read-only) | scratch `stall-query_kn_{1,2}.txt` `73a01663` / `83910288` | slow_request clusters, RSS |

The analysis scripts are offline, read files only, and are not in the repo. They live in the session scratchpad
`/tmp/claude-1001/-home-bobby/798e80a2-9f96-4c92-b157-75b07ed3460c/scratchpad/optres/`:

| Script | sha256 prefix | What it computes |
| --- | --- | --- |
| `analyse.py` | `55447d40` | throttle per role, intervals containing the tails, ingestor 1 s samples, core `commit_ms`, cleaner episodes |
| `around.py` | `c7325a00` | throttle around an instant |
| `cleaner_split.py` | `5c062539` | broker throttle inside vs outside its own md.latest cleaner episodes |
| `gc.py` | `54386a2d` | G1 pause statistics |
| `evidence.py` | `0ccad5cc` | rc17v2 evidence file |
| `svc_diff.py` | `086eea80` | candidate diff |

The four G4 tail instants come from the A4 draft section 1.1: 12:36:05.7, 12:40:27.0, 12:44:21.8 and
13:01:00.7-01.1Z. venue->DL is 28-33 ms on every failing event. The internal segments are receive->CreateTime
154-295 ms and CreateTime->observer 115-206 ms.

### 2.1 Throttling per role over the load **[verified]**

Source: host.json, 143 intervals of ~12.6 s. "Throttled" is the `throttled_usec` delta; "periods %" is
`nr_throttled / nr_periods` in the worst interval.

| Role | Cap | Intervals throttled | Throttled total | Worst interval | Worst periods % | Mean vCPU | Max vCPU (12.6 s) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kafka1 | 1.75 | 123 / 143 | 39.7 s | 1,701 ms | 24.6 | 1.043 | 1.238 |
| **kafka2** | 1.75 | **131 / 143** | **116.1 s (6.3 % of wall)** | **2,790 ms** | **29.6** | 1.244 | 1.405 |
| kafka3 | 1.75 | 109 / 143 | 12.6 s | 524 ms | 12.0 | 0.810 | 0.958 |
| query_kn_1 | 1.5 | 136 / 143 | 6.7 s | 183 ms | 5.6 | 0.547 | 0.767 |
| **query_kn_2** | 1.5 | **143 / 143** | 26.2 s | 542 ms | 9.6 | 0.707 | 0.892 |
| ingestor_okx_swap | 0.5 | 11 / 143 | 0.61 s | 177 ms | 1.6 | 0.081 | 0.109 |
| ingestor_binance_usdm | 0.75 | 1 / 143 | 0.06 s | 61 ms | 0.8 | 0.088 | 0.122 |
| binance_bar_edge | 0.75 | 40 / 143 | 4.6 s | 332 ms | 51.7 | 0.019 | 0.213 |
| stream_kn_1 / _2 | 0.75 | 1 / 2 | 0.06 / 0.04 s | 62 / 28 ms | 0.8 | 0.195 / 0.151 | - |
| rust_core x3, projectors x2, market_cache, stable_redis | - | 0 | 0 | 0 | 0 | - | - |

The throttling is bursty, not saturation. kafka2 averages 71 % of its cap and is still throttled in 92 % of the
intervals. The CFS quota is enforced per 100 ms period.

### 2.2 At the four G4 tail instants **[verified]**

Each row is the ~12.6 s host.json interval that contains the instant; ingestor values are 1 s samples (a3-local);
`commit_ms` is the worst 100-batch window max within -3..+5 s.

| Tail | kafka2 throttled | kafka1 / kafka3 | OKX ingestor (1 s, offset from tail) | Binance ingestor run-queue (no throttle) | core `commit_ms` max (load p50 77.8, p99 143.7) | Cleaner phase on a broker |
| --- | --- | --- | --- | --- | --- | --- |
| 12:36:05.7 | 144 ms | 157 / 14 ms | +1.27 s: throttled 68 ms, run-queue 80.6 ms | 66 ms | 129.4 | none |
| 12:40:27.0 | **1,467 ms (23 / 127 periods)** | 12 / 223 ms | +0.44 s: **throttled 177 ms (2 periods)**, usage 280 ms/s, run-queue 127 ms | 160 ms | **154.0** | 0.3 s md.latest.v2-1 pass on kafka3 only |
| 12:44:21.8 | **873 ms** | 0 / 45 ms | no throttle, run-queue 34 ms | 65 ms | 124.7 | **inside md.latest.v2-2 on kafka2 + kafka3 (12:42:07-12:46:15)** |
| 13:01:00.7 | 192 ms | 78 / 1 ms | +0.96 s: throttled 50 ms, usage 253 ms/s, run-queue 133 ms | 147-176 ms | **197.5** (load max) | **inside md.latest.v2-4 on kafka1 (12:58:08-13:01:18)** |

Over the load: the OKX ingestor had 12 throttled 1 s samples (606 ms in total). Usage per second is p50 78 / p99 138
/ max 280 ms against a 500 ms/s quota; run-queue max-thread p99 46.8 ms, max 132.7 ms. The Binance ingestor had 1
throttled sample, and its run-queue reached 175.6 ms with **no** throttling.

### 2.3 What the cleaner adds on the brokers **[verified]**

Source: `cleaner_split.py`. Mean throttled ms per interval, split by whether the broker ran its own md.latest
cleaner episode:

| Broker | Inside its cleaner episodes (n, mean, max) | Outside (n, mean, max) | Mean vCPU inside / outside |
| --- | --- | --- | --- |
| kafka1 | 48, **650.8**, 1,701 | 95, 89.4, 1,023 | 1.104 / 1.011 |
| kafka2 | 68, **1,061.7**, 2,421 | 75, 585.2, 2,790 | 1.268 / 1.222 |
| kafka3 | 66, 101.1, 482 | 77, 76.8, 524 | 0.830 / 0.793 |

The cleaner adds only +0.04-0.09 vCPU of mean draw, but it multiplies broker throttling by 1.3-7x. kafka2 is
throttled even outside the cleaner (585 ms per interval). So the broker quota is the common factor, and the cleaner
is an aggravator. This matches the bafbe91 RCA.

### 2.4 Broker GC (load window) **[verified]**

| Broker | Pauses (rate) | p50 / p99 / max | Total pause | Remark+Cleanup | Heap committed max |
| --- | --- | --- | --- | --- | --- |
| kafka1 | 1,449 (0.81/s) | 13.9 / 26.3 / 45.9 ms | 19.7 s (1.09 %) | 220 | 618 MiB |
| kafka2 | 1,458 (0.81/s) | 14.5 / 35.2 / 51.7 ms | 21.0 s (1.17 %) | 138 | 645 MiB |
| kafka3 | 1,225 (0.68/s) | 8.9 / 21.0 / 53.8 ms | 10.4 s (0.58 %) | 490 | 515 MiB |

At the tails: kafka1 Young 39.5 ms at 13:01:00.785 (+0.09 s), and kafka3 Young 38.2 ms at 12:36:05.742 (+0.04 s).
Both are among the run's top-5 pauses. Pauses are at most ~54 ms against internal tails of 270-530 ms, so GC is a
contributor, not a sufficient cause. That agrees with the bafbe91 RCA (10-26 ms at ~0.7/s, single pauses max 63 ms).

### 2.5 Query (G2 MARK path) **[verified]**

- `qdl.stall` in the load:
  - query_kn_1: 25 slow_request + 1 loop_lag.
  - query_kn_2: 105 slow_request + 2 loop_lag.

  Almost all of them fall in **one cluster, 12:43:46-12:43:53Z, on both replicas**:
  - paths: snapshot 87, reference:batch 39 (the MARK reference read path), warmup 4;
  - the warmups took 3.54 s (kn_1) and 4.57 s (kn_2);
  - loop lag 52-79 ms.
- The driver READ outliers in the load are 139: query_v2_2 104, query_v2_1 35. Of these, 114 fall in
  12:43:46-52, including 22 of the 35 `MARK_INDEX_REFERENCE|BINANCE` outliers (max 366.5 ms in the driver's read).
- In the same seconds (12:43:25-12:44:03) both replicas hit their highest 12.6 s CPU of the load (kn_1 0.767,
  kn_2 0.892) and were throttled:
  - query_kn_2: 438 + 299 ms in two consecutive intervals;
  - query_kn_1: 183 ms.
- Memory, G14 table: query_kn_1 mem_max **1,412.1 / 1,536 MiB (headroom 8.1 %)**, query_kn_2 1,240.1 (19.3 %).
  query_kn_1's own `qdl.stall` line at 12:34:47.876Z reports `rss_mib 1548.5`.

  Now (~16:04Z, no load): kn_1 1.152 GiB, kn_2 994 MiB. So the memory is load-driven, not a monotonic leak
  **[verified by two points only]**.
- **[inferred]** The G2 DOGE max (1,082.9 ms) has no per-sample timestamp in the trace basis (A4 draft 1.2). So
  placing it in the 12:43:4x cluster is plausible, not shown. The cluster looks driven by warmup serialization in
  the event loop (multi-second warmups plus loop lag). Throttling adds about 0.3-0.44 s per 12.6 s interval on top.

### 2.6 Host

| Item | Value | Source |
| --- | --- | --- |
| vCPU / RAM | 16 / 30 GiB, no swap | `nproc`, `free -g` at 15:54Z: used 17, available 13 GiB |
| load1 during the counted load | p50 17.33, max 22.49 (pre-load p50 13.78) | guard host.jsonl |
| CPU PSI (now, no load) | some avg10 5.77 / avg60 5.43 / avg300 6.22 | `/proc/pressure/cpu` 15:54Z |
| Measured draw in the load | DL 5.63 vCPU (G14 mean sum) + TS writer 1.35 + driver ~1.18 (2,250 CPU-s / ~1,900 s) = **8.16 vCPU** | scorecard G14, receipt `worker_cpu_seconds` |
| One 5 s `/proc/stat` sample at ~16:04Z, no load | ~10.3 vCPU busy, iowait ~9.7 % | single sample, noisy; it overlapped the start of a Codex lock window, so treat it as indicative only |

## 3. Per-role proposal with evidence, effect and rollback

### 3.1 Kafka brokers: 1.75 -> 2.25 vCPU (x3)

- **Evidence.**
  - kafka2 is the most throttled role in the stack (2.1 above).
  - Its throttling peaks at the two tails with the largest core commit stalls: 1,467 ms at 12:40:27 with
    `commit_ms` 154, and 873 ms inside the 12:44 cleaner phase.
  - kafka2 holds the core group coordinator. The 10-16 B roll measured the first real production load of
    `__consumer_offsets-43` on broker 2. Leader rebalance has been off since then, and no broker has restarted
    (RestartCount 0, StartedAt 10-16). Commit, TxnOffsetCommit and the markers of all three cores therefore run
    through kafka2. This is **[inferred from the plan record]**. Preflight step 0.6 verifies it with a read-only
    describe.
- **Why 2.25 and not 2.0.**
  - Precedent (10-14, `docker update`, no restart): kafka1 at 0.97 core mean was raised 1.25 -> 1.75, which is
    55 % of the cap. Its throttle went from 28.8 % to 0.17 % of periods.
  - kafka2's mean is 1.244. 1.244 / 0.55 = 2.26, so 2.25 is the quarter-grid value that puts kafka2 at that same
    measured ratio. At 2.0 kafka2 would sit at 62 %.
  - The cap stays uniform across brokers, because leadership moves on any failover.
- **Expected effect [inferred].**
  - kafka2 throttled periods fall from up to 29.6 % per interval to low single digits.
  - The commit stalls (core `commit_ms` p99 143.7 ms, max 197.5 ms) lose their throttle component.
  - This is the CreateTime -> observer segment (115-206 ms at the tails).
  - The in-cleaner increment (+0.04-0.09 vCPU) fits inside the +0.5.
  - GC pauses stop being stretched by throttling. The JVM thread pools stay sized from the startup quota, because
    there is no restart.
- **Draw.** The mean rises at most by the throttled share: kafka1 0.022 + kafka2 0.064 + kafka3 0.007 = **+0.09 vCPU**.
- **Blast radius.** `docker update` changes `cpu.max` live. No restart, no leadership move, no client
  reconnect. Precedent: 10-14 13:47:40Z.
- **Rollback.** `docker update --cpus 1.75` on each broker. Immediate, no restart.

### 3.2 Query: 1.5 -> 2.0 vCPU and 1.5 -> 2 GiB (x2)

- **Evidence.**
  - query_kn_2 is throttled in 143 / 143 intervals and query_kn_1 in 136 / 143.
  - Both are throttled and at their CPU maximum during the 12:43:46-53 slow_request cluster (2.5). That cluster
    holds 82 % of the driver READ outliers, including the MARK BINANCE reference reads.
  - query_kn_1 memory headroom is 8.1 %, with `rss_mib` 1,548.5 at load start.
  - The memory rise is load-driven: 1.152 GiB now versus 1,412 MiB in the load.
- **Expected effect [inferred].**
  - CPU: removes the throttle share of the cluster (~0.3-0.44 s per 12.6 s). It does **not** remove event-loop
    blocking by warmup serialization. One Python process with 67 threads and a GIL cannot use more than about one
    core of bytecode time; the cap only helps the native/TLS threads. So the G2 MARK BINANCE max may move only a
    little. The code path is IDR-119, together with the per-read provenance work of IDR-126.
  - Memory: query_kn_1 headroom becomes ~31 % (1,412 / 2,048). That removes the G14 FAIL condition (mem_max >=
    limit) and the OOM-kill risk at this load.
- **Draw.** CPU +0.018 vCPU (throttled share). RAM: limits +1 GiB; actual use grows only if the process grows.
- **Blast radius.** Live `docker update`; no restart, no reconnect.
- **Rollback.**
  - `docker update --cpus 1.5` at any time.
  - Memory back to `--memory 1536m --memory-swap 3g` **only while the container uses <= 1.3 GiB**
    (`docker stats --no-stream`). Lowering the limit under the current usage forces reclaim or OOM.
  - Otherwise keep the memory. A higher limit has no latency downside.

### 3.3 ingestor_okx_swap: 0.5 -> 1.0 vCPU

- **Evidence.**
  - 3 of 4 tail instants have a CFS throttle within +0.44..+1.27 s: 68, 177 and 50 ms, in 1-2 periods each.
  - In those seconds usage is 147-280 ms/s against a p50 of 78, and the worst-thread run-queue is 80-133 ms
    against a p99 of 46.8.
  - In the 1 s sample just after the tail, at 12:40:27 and 13:01:00.7, the receive -> CreateTime segment
    (154-295 ms) is inside DL's ingestor.
- **Why 1.0 and not 0.75.**
  - A throttled period means more than 50 ms was consumed in one 100 ms period. The hot work is one parse/produce
    burst.
  - At 1.0 a single thread can never be throttled; at 0.75 a single-thread burst of more than 75 ms still is.
  - The rc17v2 T4 rule would require exactly 0.75 if its trigger were met. Its trigger is not met (section 8), so
    T4 cannot be the vehicle either way.
- **Expected effect [inferred].** It removes the throttle part of the receive -> CreateTime tail. It does **not**
  remove host run-queue wait: the Binance ingestor shows 147-176 ms run-queue at the same instants with zero
  throttling. That wait is host CPU contention (load1 p50 17.3 on 16 vCPU), which no single cap fixes.
- **Draw.** +0.0003 vCPU mean (606 ms throttled / 1,800 s). Total raise over the frozen T4 baseline (0.5 / 0.5):
  Binance +0.25 (already deployed) + OKX +0.5 = +0.75, within the +1.5 envelope of capacity step 1.
- **Order rule.** The ingestor is a producer, so it is raised **after** the brokers (R1.29: producers never ahead of
  consumers).
- **Blast radius.** Live `docker update`; no lane reconnect.
- **Rollback.** `docker update --cpus 0.5`.

## 4. G14 impact (frozen rule)

Frozen rule, TS `upgrade/p184-phase-d-gates.md:42` (worktree `trading-system-next-upgrade`):

> | G14 | Resources | every DL role and the TS writer | CPU/RAM at stage 100 recorded per role with headroom;
> the budget is set by the owner from the measurement (no fixed <= 5 vCPU, owner 2026-10-08) | owner amendment
> 2026-10-08 |

Scorer: `scripts/p184_gate_campaign.py:1678-1689` and `scripts/p184_g14_resource_table.py:56-101`. It returns:

- **FAIL** only when a role has `mem_max_mib >= mem_limit_mib`;
- **NOT_MEASURED** when an expected role has no samples;
- **PASS** otherwise.

CPU is recorded (mean / p95 / max / throttled share) but never judged.

Why this package cannot make G14 FAIL:

- It raises limits only and lowers none.
- `docker update` keeps every container name and id, so the monitored list keeps all 18 roles.
- The only near-limit role today (query_kn_1, 8.1 % headroom) gains ~23 points of headroom.

Values G14 will record after the change:

| Role | Limit |
| --- | --- |
| kafka1 / kafka2 / kafka3 | cpu 2.25; mem 1,536 / 2,048 / 1,536 MiB |
| query_kn_1 / query_kn_2 | cpu 2.0; mem 2,048 MiB |
| ingestor_okx_swap | cpu 1.0; mem 256 MiB |

The expected `dl_cpu_mean_sum_vcpu` is about 5.74 (5.63 + ~0.11) **[inferred]**. That is under the 6.5 vCPU ceiling
of the 10-12 capacity package. The 09-18 "<= 5.0" rule was superseded for G14 by the owner amendment of 10-08.

## 5. Not proposed, with reasons

### 5.1 C1, cleaner I/O 4 -> 3 MiB/s: out of this bundle

Tool dry-run (section 9.3): it keeps up, duty 0.662 (capacity 1.541 MB/s against demand 1.02).

Reasons:

1. The broker quota raise already covers the measured in-cleaner increment (+0.04-0.09 vCPU) with a 0.5 vCPU margin.
2. C1 lengthens every cleaner phase by about 4/3. kafka2 was already in a cleaner episode in 68 of 143 intervals, so
   the exposure window grows while only the intensity falls. Its sign on the commit tail was never A/B'd (G4 draft
   C1).
3. Slower compaction of `__consumer_offsets` raises the coordinator-load cost of a broker move. That is the G12
   mechanism of the 10-16 RCA: ~4.8 s for partition 43 on broker 2.
4. In a single bundled measurement, its effect cannot be told apart from the quota raise.
5. **C1 would change the G12 production-parity hash.** `scripts/p184_g12_isolated_fault.py:3288-3300` (D2)
   describes `log.cleaner.*`, including `log.cleaner.io.max.bytes.per.second`, as the cluster default plus every
   broker's overrides: "A throttle-only change now changes the hash". C1 therefore puts the G12 credit at risk.
   CPU and memory caps are not in that snapshot.

Fallback: if the final run still shows in-cleaner throttling at the tails, C1 is the next single variable. Use the
tool exactly as dry-run below.

Cleaner threads stay at 1 and the dedupe buffer at 128 MiB (kafka1 startup `KafkaConfig values`, read from docker
logs). Adding threads would add concurrent cleaner CPU, which is the wrong direction.

### 5.2 Kafka heap and broker memory: not in this bundle

- Broker memory: mem_max 948 / 1,014 / 827 MiB against limits of 1.5 / 2 / 1.5 GiB (38-51 % headroom). No role is
  near its limit, so there is no evidence for broker RAM.
- GC is a contributor (2.4), with max pauses of ~40-54 ms against tails of 270-530 ms.
- A heap change (for example `-Xms1g -Xmx1g` plus a 2 GiB limit on kafka1/3) needs a rolling restart of all three
  brokers. That is a maintenance interruption. The precedent cost was a core commit stall of 2.5 / 3.3 / 5.2 s per
  roll, plus coordinator loads.
- It also needs a new roll packet: `scripts/kafka_auto_leader_rebalance_roll.py` is single-purpose (its `change`
  is the `KAFKA_AUTO_LEADER_REBALANCE_ENABLE` env).
- The quota raise also shortens throttle-stretched pauses.

If the owner wants it anyway, section 10 gives the procedure.

### 5.3 Roles left as they are

- **ingestor_binance_usdm** (0.75): 1 throttled second in the load. Its run-queue spikes are host contention without
  throttling. It is green, so it is not touched (R1.29).
- **rust_core x3, market_projector x2, stream_kn x2, market_cache, stable_redis**: 0 throttled intervals at every
  tail (stream: 1-2 intervals of <= 62 ms in the whole load).

  Observation, not a proposal: `rust_core_3` runs at 0.5 vCPU while `rust_core` and `rust_core_2` run at 1.0. Its
  mean is 0.093 with 0 throttling and its `commit_ms` p99 is 138.5, the same as its peers. Its commit stalls are
  broker-side.
- **binance_bar_edge** (0.75): throttled at minute boundaries (40 intervals, up to 51.7 % of periods, mean 0.019).
  It is a producer into Kafka at exactly the boundary seconds. Raising it would push its burst harder into the
  brokers (producers are not raised ahead of consumers), and no failing row of the counted run is a bar row.
- **TS writer `market_data_service_p184w`**: no cap (NanoCpus 0). Not touched (G12/G13 credits).
- No cuts are offered to make the package resource-neutral. Every other role is green, and the owner's 10-18
  decision approves an increase. Net: ceilings +3.0 vCPU, limits +1 GiB.

## 6. Host headroom

| | Before | After (ceilings) | After (expected draw) |
| --- | --- | --- | --- |
| DL CPU ceiling sum | 17.25 | 20.25 | - |
| DL mean draw in a stage-100 | 5.63 | - | ~5.74 **[inferred]** (+ throttled share: Kafka 0.093, Query 0.018, OKX 0.0003) |
| Measured parties in the load (DL + TS writer + driver) | 8.16 | - | ~8.27 of 16 vCPU |
| DL memory limit sum | 21,408 MiB | 22,432 MiB | DL mem_max sum 7,719 MiB in the load (market_cache uses 1.18 of 9 GiB) |
| Host RAM available | 13 GiB (15:54Z) | - | >= 12 GiB after the +1 GiB query limits are fully used |

Ceilings are not reservations. The risk is in **simultaneous bursts**, not the mean. The brokers can now take
3 x 2.25 = 6.75 vCPU together in one 100 ms period, against 5.25 before. They take it on a host that already runs
at load1 17-22 in the load, from the TS writer (no cap), the driver, alphas and portal (portal caps sum 15 vCPU).

This is the 09-18 pattern: raising ceilings moved contention, and p50 went from 475 ms to 17.9 s. The mitigations
are:

- the raises are limited to the roles with measured throttling at the tails;
- the producer (ingestor) is raised last;
- a HOLD on target metrics follows every step;
- rollback is instant (no restart).

TS, alpha and OS keep ~7.7 vCPU of mean headroom and >= 12 GiB RAM.

## 7. One-variable rule vs this bundle

R1.29 (09-18) asks for one variable per >= 10 min window, decided by the target metric. The owner's 10-18 decision
bundles the package and measures the gate once. This packet keeps each step a separate variable, with its own 10-min
HOLD on target and diagnostic metrics, so a regression stops and reverts that step alone. The final stage-100 then
scores the bundle and cannot attribute its effect to any single step. That limit is stated, not hidden.

## 8. Interaction with the rustls rust-image roll and the roll tools

- `scripts/kn_production_packet.service_from_inspect` (lines 66-71) builds the compose service from the **live
  inspect**, including `NanoCpus` -> `cpus` and `Memory` -> `mem_limit` (`MemorySwap` is not carried; compose
  defaults it to 2x).
- So an rc17v2 or rc15 recreate **after** a `docker update` carries the new cap. The candidate's expected delta
  stays `['image']`, and `svc_diff.py` (9.2) shows the exact fields.
- **Recommended order:** run the rust-image roll first (its own packet, own HOLD), then the resource steps.
- **Rollback trap:** a rollback compose saved by a roll *before* the resource step holds the **old** caps. For
  example, rc17v2's saved compose for `ingestor_okx_swap` holds 0.5. Recovering with it reverts the cap. After any
  image rollback, re-apply step K/Q/I for that role and verify with `docker inspect`. The same holds for the
  10-17 rc15 Query composes, which hold 1.5 vCPU / 1.5 GiB.
- **rc17v2 cannot carry the OKX raise.**
  - Dry-run (9.1): `evidence_does_not_justify_raise`. The 30 load windows peak at usage 0.090 cores and throttle
    ratio 0.0050, under the frozen T4 triggers (> 70 % of the cap, or > 5 % of periods).
  - T4 also binds a raise to an image-unchanged recreate (`current["Image"] == image_id`) and to the quarter-step
    rule (0.75).
  - The 1.0 raise is therefore an owner-approved `docker update` **outside T4**, recorded here. T4's frozen
    cumulative budget is still respected (+0.75 of +1.5).

## 9. Dry-run outputs (no mutation)

### 9.1 rc17v2 (`rollout_rc17_v2.py` sha256 `8de00c46...`), saved inspects, evidence from the 123222Z a3-local windows

```
ROLL_DRY_RUN=1 ROLL_INSPECT_DIR=<scratch>/inspect ROLL_CPUS=1.0 ROLL_CPU_EVIDENCE=<scratch>/okx-cpu-evidence.json \
  python3 -B rollout_rc17_v2.py sha256:a9679def... ingestor_okx_swap
{"detail": {"throttle": 0.004991680532445923, "usage": 0.0904405603713625}, "phase": "cpu",
 "reason": "evidence_does_not_justify_raise", "result": "refused_before_mutation", "role": "ingestor_okx_swap"}
{"exit": 4, "not_attempted": [], "stopped_after": "ingestor_okx_swap"}
```

ROLL_CPUS=0.75 gives the same refusal. No receipt is written; nothing appeared under the rc17 receipt root.

### 9.2 Candidate diff that a later rc15 / rc17v2 recreate would carry (pure, saved inspects)

```
query_kn_1 diff ['cpus', 'mem_limit'] {'cpus': (1.5, 2.0), 'mem_limit': (1610612736, 2147483648)} mem_swap(inspect) 3221225472
query_kn_2 diff ['cpus', 'mem_limit'] {'cpus': (1.5, 2.0), 'mem_limit': (1610612736, 2147483648)} mem_swap(inspect) 3221225472
kafka1 diff ['cpus'] {'cpus': (1.75, 2.25)} mem_swap(inspect) 3221225472
kafka2 diff ['cpus'] {'cpus': (1.75, 2.25)} mem_swap(inspect) 4294967296
kafka3 diff ['cpus'] {'cpus': (1.75, 2.25)} mem_swap(inspect) 3221225472
ingestor_okx_swap diff ['cpus'] {'cpus': (0.5, 1.0)} mem_swap(inspect) 536870912
```

`docker update` has no dry-run mode. The commands in section 11 are the full mutation set.

### 9.3 Cleaner tool (`scripts/kafka_cleaner_io_throttle.py --rate 3145728`, dry-run default; C1 not proposed)

```
"keep_up": {"rate_bytes_s": 3145728, "processed_capacity_mb_s": 1.541, "demand_mb_s": 1.02, "duty": 0.662, "keeps_up": true}
alter:    kafka-configs.sh --alter --entity-type brokers --entity-default --add-config log.cleaner.io.max.bytes.per.second=3145728
rollback: ... --add-config log.cleaner.io.max.bytes.per.second=4194304
```

### 9.4 Live Kafka config, read without a container

The read-only admin `kafka-configs --describe` was **not** run. The host lock was held by other agents during this
work: Codex PMA11 16:03:59-16:08:59Z, then `claude-dl-rustls-0.23.46` 16:06-18:06Z. The admin runner starts a
container, and it refuses under a foreign lock. Instead, from `docker logs` of kafka1:

- startup `KafkaConfig values` (10-16 14:14Z): `log.cleaner.threads = 1`, `log.cleaner.dedupe.buffer.size =
  134217728`, `num.io.threads = 8`, `num.network.threads = 3`, `num.replica.fetchers = 1`;
- `DynamicConfigPublisher ... log.cleaner.io.max.bytes.per.second -> 4194304`.

KRaft quorum leader: kafka1 since 2026-10-16 14:25:29Z (epoch 151; the last transition lines on all three brokers).

## 10. Rolling broker restart procedure (only if the owner adds a heap change; not part of the proposal)

This is a declared maintenance interruption, on the precedent of the B roll of 10-16. The B tool cannot do it as
is, so a heap-capable roll packet must first be prepared and reviewed. The procedure is:

1. Hold the host lock as the roll owner. No other step may be in flight. Run `kafka_b_roll_runbook.py check` with an
   equivalent read-only check, and confirm 0 URP.
2. Re-read the quorum leader at run time. Roll **non-leaders first and the quorum leader last** (today kafka1). If
   kafka2 still holds the core group coordinator, roll it before the leader, with the coordinator load measured.
3. For each broker:
   - recreate with the new env and memory;
   - wait for healthy (~20 s) and for ISR to rejoin (~4 s in the precedent);
   - collect evidence: core `commit_ms` per progress line, `Finished loading` lines, restart counts, writer READY.
4. HOLD between brokers:
   - writer READY 64/64;
   - 0 eligibility loss;
   - core retries 0;
   - no URP;
   - stall within the precedent (<= 5.2 s).

   On a breach, restore that broker from its saved rollback compose and STOP.
5. `elect --scope data` only. Coordinator moves are refused unless the measured `__consumer_offsets-43` load is
   within budget.
6. Run the final-check parity snapshot: `auto.leader.rebalance.enable` false; C and A' unchanged.

## 11. Procedure (the proposal)

```
cd /home/bobby/.worktrees/data-layer-p184-closure
P=~/.local/state/qdl-v2/releases/v2.2.4/rollout/postchecks          # hold_check.py f6605d23..., ingestor_watch.py 48c5aa08..., query_stall_report.py 812ae73f...
OUT=~/.local/state/qdl-v2/releases/v2.2.4/opt-resource-20261018; mkdir -p $OUT
C=qdl_v2_stable_candidate
```

### Step 0: preconditions (stop if any fails)

1. Owner approval of this packet. The 10-18 decision covers the resource increase; this packet fixes the exact
   values.
2. The rust-image roll is done and its HOLD passed, or it is explicitly deferred.
3. Host lock:
   - `python3 -B scripts/host_measurement_lock.py check` reports exit 0 (no foreign job, no valid foreign lock);
   - then `python3 -B scripts/host_measurement_lock.py acquire --owner claude-dl-opt-resource --purpose "OPT
     resource package K/Q/I + HOLDs" --minutes 90`.
4. No `ROLL_*` variable in the shell: `env | grep ^ROLL_` prints nothing.
5. The live caps still equal section 1:

   ```
   for r in kafka1 kafka2 kafka3 query_kn_1 query_kn_2 ingestor_okx_swap; do
     docker inspect -f '{{.Name}} {{.Id}} {{.HostConfig.NanoCpus}} {{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.RestartCount}}' $C-$r-1
   done | tee $OUT/inspect-before.txt
   ```

   Expected values:

   | Role | NanoCpus | Memory | MemorySwap |
   | --- | --- | --- | --- |
   | kafka1 / kafka3 | 1750000000 | 1610612736 | 3221225472 |
   | kafka2 | 1750000000 | 2147483648 | 4294967296 |
   | query_kn_1 / query_kn_2 | 1500000000 | 1610612736 | 3221225472 |
   | ingestor_okx_swap | 500000000 | 268435456 | 536870912 |

   RestartCount is 0 for every role. The ingestor ids may differ after the rust roll; then record the new ids.
6. Coordinator check, read-only. This is the one admin container of the procedure, inside our own lock: describe the
   group coordinator of the core group and the `__transaction_state` leaders. Record which broker is the coordinator.
   It is informational: it does not change the plan, which is uniform.
7. Baseline, 10 minutes, nothing changed:
   - `S0=$(date -u +%s); python3 -B $P/hold_check.py $S0 $OUT/hold-baseline.json` shows `READY 64 64` and
     `violations []`;
   - `python3 -B $P/ingestor_watch.py ingestor_okx_swap 600 > $OUT/watch-baseline.jsonl` records core
     `raw_age_websocket_ms` (the target-side age);
   - run the cgroup sampler below, `cg 600 > $OUT/cg-baseline.json`.

   cgroup sampler, read-only, host cgroup files. It prints usage vCPU and the throttled-period share per role over N
   seconds:

   ```
   cg() { python3 -B - "$1" <<'EOF'
   import json, subprocess, sys, time
   roles = ["kafka1","kafka2","kafka3","query_kn_1","query_kn_2","ingestor_okx_swap","ingestor_binance_usdm"]
   ids = {r: subprocess.check_output(["docker","inspect","-f","{{.Id}}",f"qdl_v2_stable_candidate-{r}-1"],text=True).strip() for r in roles}
   rd = lambda i: dict(l.split() for l in open(f"/sys/fs/cgroup/system.slice/docker-{i}.scope/cpu.stat"))
   a = {r: rd(i) for r, i in ids.items()}; t0 = time.time(); time.sleep(float(sys.argv[1])); b = {r: rd(i) for r, i in ids.items()}; w = time.time() - t0
   print(json.dumps({r: {"vcpu": round((int(b[r]["usage_usec"])-int(a[r]["usage_usec"]))/1e6/w,3),
     "throttled_periods_pct": round(100*(int(b[r]["nr_throttled"])-int(a[r]["nr_throttled"]))/max(1,int(b[r]["nr_periods"])-int(a[r]["nr_periods"])),2),
     "throttled_ms": round((int(b[r]["throttled_usec"])-int(a[r]["throttled_usec"]))/1e3,1)} for r in roles}))
   EOF
   }
   ```

   Also record the host: `cat /proc/pressure/cpu; uptime`.

### Step K: Kafka brokers 1.75 -> 2.25

```
for b in kafka2 kafka1 kafka3; do docker update --cpus 2.25 $C-$b-1; done
for b in kafka1 kafka2 kafka3; do docker inspect -f '{{.Name}} {{.HostConfig.NanoCpus}} {{.RestartCount}}' $C-$b-1; done   # 2250000000, 0
```

HOLD K, 10 min:

- `S=$(date -u +%s); cg 600 > $OUT/cg-K.json; python3 -B $P/hold_check.py $S $OUT/hold-K.json $OUT/hold-baseline.json`
- `python3 -B $P/ingestor_watch.py ingestor_okx_swap 600 > $OUT/watch-K.jsonl`, run in parallel with the sampler.

Pass when all of these hold:

- `violations []`, `READY 64 64`, RestartCount unchanged;
- core retry/rebalance lines 0;
- core `raw_age_websocket_ms` max not above the baseline max x 1.2;
- host CPU PSI `some avg60` not above the baseline + 5 points.

Diagnostic only: kafka2 `throttled_periods_pct` falls.

On a breach: `docker update --cpus 1.75` on all three, then STOP. Revert only after the core lag and stream
`lagging` are back at the baseline, not while a backlog drains (R1.29).

### Step Q: Query 1.5 -> 2.0 vCPU, 1.5 -> 2 GiB

```
for q in query_kn_1 query_kn_2; do docker update --cpus 2.0 --memory 2g --memory-swap 4g $C-$q-1; done
for q in query_kn_1 query_kn_2; do docker inspect -f '{{.Name}} {{.HostConfig.NanoCpus}} {{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.RestartCount}}' $C-$q-1; done
# 2000000000 2147483648 4294967296 0
```

HOLD Q, 10 min:

- same as HOLD K;
- plus `python3 -B $P/query_stall_report.py query_kn_1 $S` and the same for query_kn_2: no Traceback, no new
  refusal class, health OK.

On a breach:

- `docker update --cpus 1.5` on both;
- memory back to `--memory 1536m --memory-swap 3g` only if `docker stats --no-stream` shows <= 1.3 GiB, else keep
  2 GiB;
- STOP.

### Step I: ingestor_okx_swap 0.5 -> 1.0

```
docker update --cpus 1.0 $C-ingestor_okx_swap-1
docker inspect -f '{{.Name}} {{.HostConfig.NanoCpus}} {{.RestartCount}}' $C-ingestor_okx_swap-1   # 1000000000 0
```

HOLD I, 10 min:

- same as HOLD K;
- plus `ingestor_watch.py ingestor_okx_swap 600`: no lane reconnect caused by the update, OKX venue lag unchanged or
  lower.

On a breach: `docker update --cpus 0.5`, then STOP.

### Step R: record (after the three HOLDs pass)

1. `docker inspect` of the 6 roles goes to `$OUT/inspect-after.txt`. Release the lock:
   `python3 -B scripts/host_measurement_lock.py release --owner claude-dl-opt-resource`.
2. Compose record, one config commit:
   - `docker-compose.v2-stable.yml`: kafka1-3 `cpus: 2.25`; query_v2_1/2 `cpus: 2.00`, `mem_limit: 2g`;
     ingestor_okx_swap `cpus: 1.00`;
   - `drafts/compose-v2-stable-release-draft.diff` regenerated;
   - each value carries a comment naming this packet and the apply time.

   Without this, a future `compose up` of those services would silently revert the caps.
3. DL plan journal entry and `CERTIFICATION_LEDGER` row (resource state of the final run).

### Step F: final stage-100 (once, on the rolled images plus these caps)

- The same campaign protocol as 123222Z: own lock, guard, clean host.
- **Scoring note.** A4 (`config/v2/p184-stage100-a4-ts-eligibility.json`) applies **only** to
  `p184-final-campaign-20261018T123222Z` with scorecard `b9e76a83...`. A new run is scored under A1-A3 unless A4's
  rules are re-frozen as a pre-registered amendment **before** the run.
- The owner's instruction ("nếu gate không đạt xác định là lỗi external thì nới gate") needs that decision written
  down before launch, not after.
- G12 parity inputs are untouched by this package. `p184_g12_isolated_fault.py` runs its own fixed harness
  resources: kafka 0.75 CPU / 1024m / heap 512m. Its production-parity snapshot reads the broker env rebalance flag,
  the broker image, the `log.cleaner.*` dynamic configs and the topic overrides (C, A'), and none of these changes
  here. Broker `cpus` / `memory` are not inputs.
- The G12/G13 credits then depend only on the rust-image roll's equivalence evidence.

## 12. Rollback summary (live values to restore)

| Role | Restore | Restart | Condition |
| --- | --- | --- | --- |
| kafka1 / kafka2 / kafka3 | `docker update --cpus 1.75` | no | not while a backlog drains |
| query_kn_1 / query_kn_2 | `docker update --cpus 1.5`; memory `--memory 1536m --memory-swap 3g` | no | memory only when usage <= 1.3 GiB |
| ingestor_okx_swap | `docker update --cpus 0.5` | no | - |
| compose record (step R) | `git revert` of the config commit | - | only together with the live revert |

## 13. Risks

1. **Contention moves, it does not vanish.** The 09-18 pattern: on a host at load1 17-22, higher broker burst caps
   take CPU from the TS writer (no cap) and the driver. Guarded by per-step HOLDs on target metrics and instant
   rollback.
2. **Partial effect.** Host run-queue wait and Query event-loop blocking (warmups) are not fixed by caps. G2 MARK
   BINANCE (attribution open, IDR-119 / IDR-126) and the OKX inverse maxima may stay above the pre-A4 limits. A clean
   final run can still fail on these rows, and the cause would be internal, not external. The "nới gate if external"
   path does not cover internal tails without a new owner decision.
3. **Silent revert.** Any compose-based recreate before step R, or a rollback with a pre-step saved compose (rc15
   10-17, rc17v2), restores the old caps. Mitigated by step R and by the check after every rollback.
4. **Memory rollback trap.** Lowering a Query memory limit under live usage forces reclaim or OOM. Rule in 3.2.
5. **One measurement, no attribution.** The bundle's effect is not split per step (section 7).
6. **Inferred coordinator placement.** Kafka2 as the core group coordinator is inferred from the 10-16 record. The
   uniform plan does not depend on it, and step 0.6 records the fact.

## 14. HOLD method amendment (owner-approved 2026-10-18 ~20:05Z), replaces the pass rules of section 11 K/Q/I

History. The first K application (2026-10-18 19:24:37Z) failed the section-11 rule "core raw_age max <= 1.2 x the
step-0 baseline max" (438.6 vs 197.3 ms) and showed stream_kn_1 overflow +2; it was rolled back at 19:37:12Z. Evidence
that the failure was market state, not K: the spike window held 25,920 core records against ~12,000 in a normal
window; the same 437 ms spike recurred at cap 1.75 after the rollback (19:39:34Z); at 1.75 with nothing changed the
19:43-19:53Z raw_age p99 (171.7 ms) already exceeded 1.2 x the 19:13Z baseline p99 (167.5 ms). K's target metric did
improve: broker throttled-period share 1.88 / 1.00 / 0.43 % -> 0.53 / 0.33 / 0 %.

Decisions:
- Coordinator ~19:56Z (owner rule "one variable, target metric decides"): judge on the target metric, rust_core
  raw_age p99 and commit_ms p99 (not worse by > 20 %), READY 64 64, no restart, no new error lines, no new overflow
  class (the known writer-side TRADE overflow is reported, not gating); max values report-only.
- Owner, AskUserQuestion ~20:05Z, answer "Baseline ngay trước mỗi bước (Recommended)": each step is compared with a
  fresh 10-minute baseline taken immediately before that step under the same configuration (nothing changed during it).

Implementation: `~/.local/state/qdl-v2/releases/v2.2.4/opt-resource-20261018/eval_hold2.py` (read-only; docstring holds
the exact rule): (a) K every broker's throttled share <= baseline and the sum strictly lower; Q both replicas at 2 GiB
and each throttled share <= baseline (strictly lower when > 0); I OKX ingestor throttled share <= baseline (strictly
lower when > 0); (b) p99 = nearest rank over the per-progress-line maxima of rust_core x3 in the 600 s window,
<= 1.2 x the adjacent baseline; (c) hold_check: READY 64 64, no restart, no core retry/rebalance, no error line, no
violation other than the gateway overflow counter. The window after a step serves as the next step's baseline when the
next step follows it directly (same configuration, nothing changed during it). Rollback rules unchanged (section 12).

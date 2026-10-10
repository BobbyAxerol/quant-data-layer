# P18.4 G12 / D4-R3 isolated fault acceptance - packet

Status: PACKET (written before the run). Owner approval 2026-10-10: "Duyet chay co lap" (isolated run only).
Gate: TS `upgrade/p184-phase-d-gates.md` G12 - Kafka broker / projector / Stream restart: fault cleared ->
consumers caught up to live <= 600 s, then G1-G3 inside gate; the same four timestamps as G11.
Base: `fix/p184-dl-closure` tip `5a8b063`, branch `p184-g12`.
Harness: `scripts/p184_g12_isolated_fault.py` (new; reuses DR05 material/frames/publisher envelope, DR08 Query
read view, the D4-R2 `kn_recovery_observe.KafkaAdminSource`, and the TS-owned `WriterClone` read-only from the TS
worktree). Level: `ISOLATED_FAULT`, `TEST_ONLY` provenance. Same-host rehearsal - not HA, not independent failure
domains, not production evidence.

## 1. Namespace and isolation

- Namespace: `p184-g12-dl-cfcba18d978b452f`. Every container is named `p184-g12-dl-cfcba18d978b452f-<role>` and
  labelled `p184.g12=p184-g12-dl-cfcba18d978b452f`.
- Network: one holder container (`-net`, `alpine:3.20`, `--network none`, `sleep` with a deadline) owns a
  loopback-only network namespace; every other role joins it with `--network container:<ns>-net`. No Docker
  network is created, no port is published, nothing in the namespace can reach any other container or the
  internet. Brokers listen on distinct loopback ports (9092/9093, 9192/9193, 9292/9293).
- Not touched: `qdl_v2_stable_candidate-*`, `market_data_service_p184w` (read-only `docker inspect` of its
  config only, as DR05's WriterClone does; its DL identity env is dropped and no shared secret is mounted),
  shared Redis/Kafka/Postgres, `qdl-phase3-target-*`, any container not created here. No `redis-cli MONITOR`.
  No secret or key file is printed; TLS/JWT/cursor material is freshly generated for this namespace only.
- Production guard (read-only `docker inspect`, every 60 s and at teardown): `RestartCount`/`StartedAt`/status of
  every running `qdl_v2_stable_candidate-*` container and of `market_data_service_p184w`. A restart-count increase
  or a container no longer running stops the run; a new `StartedAt` with an unchanged restart count is a recreate
  by another operator (the shared stack is not ours) and is recorded, not a stop.

## 2. Images (all already local, by ID; no build - no source change in any image)

| Role | Image ID | Tag (informational) |
|---|---|---|
| kafka-1/2/3 | `sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9` | apache/kafka (= production brokers) |
| core | `sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023` | qdl-v2-rust:2.2.1-0e00c81 (= rust_core) |
| projector | `sha256:a696cfba6ab383173607f96413c814f74c7abcc98a4c41f57c9c6e78a41bb70b` | qdl-v2-rust:2.2.1-c8015ca-projector (= market_projector_1/2) |
| stream | `sha256:1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9` | qdl-v2-rust:2.2.1-c8015ca-stream (= stream_kn_1/2) |
| query, helper, publisher, monitor | `sha256:672503b7fe87265d12af3d45fc4d344fdef0a76e94e06efe52e8dc59cab24d0a` | qdl-v2-python:2.2.3-rc3-76950c8 (= query_kn_1/2); worktree mounted read-only at /app (`git log 76950c8..HEAD -- qdl/` is empty, so library code equals the image) |
| quota + market cache Redis | `sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8` | redis (= market_cache) |
| TS writer clone x2, sampler | `sha256:15524c0bea8c51949d7ced79291d4c72722292fea2f0a5d4f8e9c7fd61dbb5ca` | local/trading-system:p184-writer-9b86ed3 (= market_data_service_p184w) |
| writer clone Redis x2 | `sha256:dfa18828...` (as above) | |
| writer clone Postgres x2 | `sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685` | postgres |
| holder | `sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc` | alpine:3.20 |

Unit tests run in `sha256:77682eea...` (qdl-v2-python:2.2.3-rc2-43bbcf0), `--network none`, read-only source.

## 3. Roles started (all `--rm`, `--pull=never`, `--read-only` where the image allows, tmpfs, bounded logs)

| Role | CPU | Memory | Notes |
|---|---|---|---|
| net (holder) | 0.05 | 16 MiB | `sleep 5400` (absolute lifetime) |
| kafka-1, kafka-2, kafka-3 | 0.75 each | 1024 MiB each (heap 256-512 MiB) | KRaft combined, RF3, min ISR 2 (data, offsets, transactions), mTLS SSL listener, data on a bind dir under the work dir so a restarted broker keeps its log |
| quota (Redis) | 0.1 | 192 MiB (maxmemory 128 MB, noeviction) | db0 quota, db1 market cache written by the projector and read by Query |
| core | 0.5 | 256 MiB | ETH BOOK bindings for BINANCE USDM and OKX SWAP (DR05 public export) |
| projector (replica a, one replica) | 0.5 | 256 MiB | stage A canonical -> latest/bars; stage B -> market cache |
| stream (one replica) | 0.5 | 512 MiB | production gateway; hot read on; read view from Query |
| query | 0.5 | 512 MiB | real KN Query read view (DR08 `serve_query`), JWT + quota |
| publisher | 0.25 | 256 MiB | test-provenance provider-shaped BOOK frames, 4 Hz per venue, idempotent acks=all |
| monitor | 0.25 | 256 MiB | 1 s: cluster/ISR, projector committed vs LSO, group members, Query/Stream port open, disk |
| sampler | 0.25 | 384 MiB | 50 ms: each TS clone's BOOK view through `MarketCacheReader` + at-use `_validate_book` |
| tswriter-binance, tswriter-okx | 2 each | 2 GiB each | TS `WriterClone` (production entrypoint and env, 4 fixed workers, ETH only, own Redis/Postgres) |
| tsredis-*, tspg-* (x2 each) | uncapped CPU | 256 MiB each | WriterClone disposables, tmpfs |
| helper (one-shot) | 0.5 | 512 MiB | material, topics, readiness probe |

Totals (caps, not reservations): ~9.2 vCPU and ~10.9 GiB. Expected draw at 8 frames/s is far below; the
measured per-container CPU/RSS is recorded every 30 s (`docker stats --no-stream`) and reported. One projector
and one Stream replica (production runs two of each): a restart is a full outage of that role - the worst case.

## 4. Faults (sequential, one stack lifetime)

0. Baseline: after both TS clones show an APPROVED book view, 300 s G1 window.
1. Kafka broker: stop the broker that leads the canonical partition (`docker stop -t 30`), keep it down 60 s,
   then start it again with the same argv and the same data dir (KRaft `format --ignore-formatted`).
2. Projector: `docker stop -t 30` the projector, start it again immediately (same argv, same epoch).
3. Stream: `docker stop -t 30` the Stream gateway, start it again immediately (same argv).

## 5. What is measured (per fault, per venue; the G11 four timestamps)

- `fault_injected_at`: the stop command is issued (outage start).
- `fault_cleared_at`: broker - the restarted broker id is back in `describe_cluster` (ISR fully restored is
  recorded separately); projector - the projector's stage-A group has a member again; Stream - TCP 8210 accepts
  again. All from the monitor; the container start time is also recorded.
- `first_valid_apply_at`: first sampler row at/after `fault_cleared_at` whose view is `APPROVED` and shows a
  frame published at/after `fault_cleared_at`.
- `caught_up_at`: first sampler row at/after `fault_cleared_at` showing a frame published at/after
  `fault_cleared_at` with view age (sample time - frame publish time) <= 1000 ms, APPROVED, and staying APPROVED
  with age <= 1000 ms for the next 30 s. Consumers caught up = the later of the two venues.
- Then a 300 s G1 window from `caught_up_at`: transit per newly observed frame (first sample showing it -
  its publish time): n, p50, p95, p99 (n >= 100), max. Also: projector lag (committed vs LSO) and when it
  reached 0, under-replicated partitions and when they reached 0, publisher delivery errors, TS heartbeat
  status / demanded / session-ready.

## 6. Pass rule

- G12 per fault: `caught_up_at - fault_cleared_at <= 600 s` for both venues, then G1 inside gate in the 300 s
  window: p95 <= 500 ms, p99 <= 1000 ms, max < 2000 ms (BOOK_DELTA/BOOK_SNAPSHOT at the TS cache).
- G2 (TRADE/QUOTE/MARK) and G3 (BAR) are NOT measured: the isolated synthetic source is BOOK-only (DR05
  material). Reported as NOT_MEASURED, not as PASS.
- Measurement resolution: the sampler polls every 50 ms; a frame skipped between two polls has no transit
  sample (it is counted as skipped, not imputed).

## 7. Stop conditions (stop at the first; evidence is kept)

- caught-up not reached within 600 s after a fault cleared (G12 FAIL);
- any role other than the faulted one exits, or the faulted role does not come back within 120 s;
- a TS clone exits; the sampler/monitor/publisher exits early;
- free disk on `/` < 20 GB (checked before start and every 10 s by the orchestrator; the monitor also logs it);
- host available memory < 3 GiB;
- any production guard breach (section 1);
- the orchestrator's absolute deadline (3600 s).

## 8. Deadlines

Orchestrator: SIGALRM at 3600 s, teardown in `finally`. Holder: `sleep 5400`. Publisher, monitor and sampler:
`--seconds` (run length) and exit cleanly at it or on SIGTERM. Every `docker` call: subprocess timeout
(15-180 s). Readiness waits: brokers 120 s, services 120 s, TS clones 300 s.

## 9. Teardown and verification

Remove (exact names, label checked before every stop): sampler, monitor, publisher, the two TS writer clones and
their Redis/Postgres (WriterClone.close), query, stream, projector, core, quota, kafka-3/2/1, net. Then:
`docker ps -a --filter label=p184.g12=<ns>` = 0 and `docker ps -a --filter name=<ns>` = 0;
`docker network ls --filter name=<ns>` = 0 (none created); `docker volume ls` count before = after (no named
volume; image VOLUMEs are overridden by tmpfs or removed with `--rm`). Private material (keys, keystore,
passwords, cursor keys) and the broker data dirs are deleted from the work dir; JSONL evidence, the verdict and
public config remain under `~/.local/state/qdl-v2/p184-g12-20261010/`. No build cache is created.

## 10. Rollback

Nothing shared changes, so there is nothing to roll back; a failed/aborted run is cleaned by
`python3 -B scripts/p184_g12_isolated_fault.py --down <work>` (exact-name, label-checked).

## 11. Result - 2026-10-10 (run 03:48-04:12 UTC)

Verdict: G12 PASS on all three faults at `ISOLATED_FAULT` level (BOOK, TS writer clone cache). G2/G3 NOT_MEASURED.
Evidence: `~/.local/state/qdl-v2/p184-g12-20261010/evidence/` (`verdict.json` from the run, `verdict.reanalyzed.json`
after the lag-predicate fix below, `publish/sampler/monitor/faults/orchestrator/stats.jsonl`, `logs/`, `teardown.json`).
Failed setup attempts kept under `p184-g12-20261010-attempts/` (a1: no output, evidence dir created after first
log line; a2/a3: brokers exited - holder had no resolvable hostname, fixed with `--hostname localhost`; a4 debug).

| Fault | injected | cleared | first valid apply (B / O) | caught up (B / O) | caught-up s | G1 window p95 / p99 / max ms (B; O) |
|---|---|---|---|---|---|---|
| broker kafka-3 (canonical leader) stop 60 s / start | 03:54:20.566 | 03:55:44.768 (ISR full 03:55:45.768) | 03:55:44.969 / .970 | 03:55:44.969 / .970 | 0.202 | 103.9 / 118.6 / 843.8; 103.8 / 120.5 / 844.3 |
| projector restart | 04:00:47.079 | 04:00:48.796 | 04:00:49.052 / .052 | 04:00:49.052 / .052 | 0.256 | 102.7 / 112.6 / 131.9; 103.6 / 117.3 / 183.4 |
| Stream restart | 04:05:51.182 | 04:05:54.827 | 04:05:57.182 / 04:05:54.960 | 04:05:57.182 / 04:05:54.960 | 2.355 | 101.8 / 111.4 / 304.7; 101.6 / 112.2 / 134.9 |

Baseline (300 s, before faults): p95 103.9 / 103.9 ms, p99 112.8 / 116.6 ms, max 139.4 / 140.1 ms; n 1,200 per venue.
Every G1 window n >= 1,198. Consumer-visible outage: broker and projector faults none (max view age 176 ms, 0
non-APPROVED samples); Stream restart BINANCE max view age 3,062 ms with 5 non-APPROVED samples, OKX 1,772 ms, 0.
Projector stage-A lag back to steady (<= 1) 4.0 s / 1.0 s / 0 s after each clear; lag peak 31 records on its restart.
Publisher 10,564 frames, 10,564 acked, 0 delivery errors, 0 resyncs. TS clone heartbeat DEGRADED with demanded 2 =
session-ready 2 throughout (BOOK-only isolated bundle; other writer feeds unavailable by construction).

Resources (docker stats, 41 samples of all 18 roles): total CPU max 162 % (mean 64 %), total memory max 2,614 MiB;
peaks kafka-3 79 % / 478 MiB, kafka-2 594 MiB, query 52 % / 152 MiB, TS writers 7 % / 264 MiB each.
Disk free 109.9 GB before, 104.8 GB at start of the final attempt, >= 97 GB after.

Teardown: 12 namespace containers + 6 WriterClone containers (2 writers, 2 Redis, 2 Postgres) + 2 log dirs removed;
by label 0, by name 0, networks 0 created/0 left, volumes 314 before -> 316 after the harness -> 2 anonymous volumes
(the WriterClone Redis `VOLUME /data`, `docker rm -f` without `-v`) removed by exact ID -> 314. Fixed in the harness:
its runner adds `-v` to WriterClone's `rm -f`. Private material and broker data dirs deleted. Production guard: 0
breaches (query_kn_1/2 were recreated by another operator at 03:43-03:44, before the final attempt's baseline).

Limits: same-host loopback rehearsal, one projector and one Stream replica, synthetic TEST_ONLY BOOK at 4 Hz/venue
(not production load), sampler resolution 50 ms. The monitor's "projector lag 0" predicate was wrong (committed
offset vs LSO keeps 1 for the commit marker); the re-analysis uses <= 1, no G12 number changed.

## 12. Final-image re-run packet - 2026-10-10 (written before the run)

Same gate, faults, measurements, pass rule, stop conditions, deadlines and teardown as sections 1 and 3-10; only
the image set and the Query code source change. Base: `fix/p184-dl-closure` tip `59e725a` plus the harness
change below. Namespace `p184-g12-dl-00e555b46f67d7c0`; evidence `~/.local/state/qdl-v2/p184-g12-final-20261010/`.

Harness change (additive): the image set is a parameter. `IMAGES` stay the defaults.
- `--image-set FILE` (JSON object `role -> sha256:<64hex>`, any subset of the roles kafka/core/projector/stream/
  python/redis/ts/postgres/holder) and `--image ROLE=sha256:<64hex>` (repeatable, applied last).
- The resolved set must match the local image IDs. It is written to `evidence/images.json` (ID, tags, OCI revision)
  and sealed in `work/handoff.json`: `g12_images`, plus `images` in DR05 role names, so the in-container
  `--material` step writes the final core digest into `core.json`.
- It is passed to every in-namespace helper as `P184_G12_IMAGES`, and `_packet` refuses a mismatch with the seal.
- Every container the run starts (restarts and the six WriterClone containers included) is `docker inspect`-ed.
  Its image ID must equal the passed value for its role, or the run stops. Rows go to `evidence/provenance.jsonl`.
- `--query-code image` runs Query from the image's own `/app` (no worktree mount). The default `worktree` keeps
  the section-2 behaviour.

Final image set (`image-set.json` in the evidence base):

| Role | Image ID | Tag / revision |
|---|---|---|
| core, projector, stream | `sha256:64468c72ce5ab36518107014bd4a3e311b96d47fad06e531f6627c46f532a1b7` | qdl-v2-rust:2.2.3-rc7d-bbfb3be (= rust_core*, market_projector_1/2, stream_kn_1/2 now) |
| python (query; helper/publisher/monitor) | `sha256:e0252f4c96da70791b0c0fd031538bff2dd7249cdf6e0fbd4399c90e2e527a2a` | qdl-v2-python:2.2.3-rc7d-bbfb3be (= query_kn_1/2 now) |
| ts (writer clones, sampler) | `sha256:34202753bbe8b74d50e45afed1cf2e7b400141324c550168462b30db220edb31` | local/trading-system:p184-writer-ed02064 (= market_data_service_p184w now) |
| kafka, redis, postgres, holder | section-2 defaults | unchanged |

Query runs the image's code (`--query-code image`). Helper, publisher and monitor are harness tooling: they run in
the final python image with the worktree harness mounted read-only. `git diff bbfb3be HEAD -- qdl generated app
scripts/p184_dr05_observer.py scripts/p184_dr08_isolated.py` is empty.

Invocation (host, stdlib only):

```
python3 -B scripts/p184_g12_isolated_fault.py --run ~/.local/state/qdl-v2/p184-g12-final-20261010 \
  --export ~/.local/state/qdl-v2/dr05-export-20261010/public-export.json \
  --namespace p184-g12-dl-00e555b46f67d7c0 \
  --image-set ~/.local/state/qdl-v2/p184-g12-final-20261010/image-set.json --query-code image
```

Before starting:
- `ps -eo args | grep -E '/run_stage100.sh [a-z] '` must be empty. If a load run is active, wait and re-check every
  2 min; give up after 60 min.
- `df /` must show >= 20 GB free.

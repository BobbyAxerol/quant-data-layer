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

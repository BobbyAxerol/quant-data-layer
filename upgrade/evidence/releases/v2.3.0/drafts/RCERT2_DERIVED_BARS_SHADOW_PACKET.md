# DL v2.3.0 R-cert 2: live shadow of DERIVED_FINAL bars (owner-approval packet)

Status: **DRAFT, NOT APPROVED, NOT RUN.** Prepared 2026-11-13 by Claude.
Journal: `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`, entry "DL v2.3.0 slice 1: R-cert 1 run, R-cert 3 offline cases, R-cert 2 packet".
Code under test: `feat/v2.3.0-derived-bars` (slice 1, `74ea7e28` plus follow-ups), merged after stage A and the declaration mechanism. That merged tree is the one the images are built from.

## 1. What this certifies

On 5 instruments the existing production native candle bindings stay untouched. The shadow derives all 13 OKX intervals from live 1m
candles: 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 2d, 3d, 1w.

The 5 instruments are BTC, ETH, SOL, DOGE and BNB-USDT-SWAP.

The window is at least 24 h and includes the Monday 2026-11-16 00:00Z boundary. That boundary closes 1w, 1d, 12h and every finer
interval at once, so it is the burst case.

Each derived bar is compared, by Decimal value, with the native OKX candle for the same open. The comparison covers o/h/l/c, `vol`,
`volCcy` and `volCcyQuote`. The native reference is fetched from public REST after the close. It is the same venue candle the
production native bindings carry.

| Gate | Pass rule |
|---|---|
| G2.1 parity | 0 mismatches on every derived bar of the window |
| G2.2 completeness | Every closed target is either derived, or recorded as a gap whose missing constituents are confirmed missing at the venue (REST shows no 1m row). A gap is never filled. |
| G2.3 latency | Median and p99 time from bar close to the derived row being readable in the shadow cache are reported per chain level. Report-only: there is no prior baseline, and the budget is set from this run. |
| G2.4 Monday burst | All 5 x (1w, 1d, 12h, ...) bars closing at 2026-11-16 00:00Z are derived and equal native. 1w opens Monday 2026-11-09 00:00Z. |
| G2.5 lineage | Every derived row is AGGREGATED. Its `aggregation` constituent count equals interval / base, and its constituent digest matches the cache rows. |
| G2.7 REST-final 1m lane (coordinator, 2026-11-13) | DOGE and BNB take their 1m base from the bar edge's REST final-bar lane (`PYTHON_REST`); the other 3 symbols use native WS. Gate: final 1m on the REST lane p99 <= 20 s after close, and every derived interval on that lane p99 <= 30 s. Parity vs native as G2.1. If it fails, the 150-tier declaration switches that universe to native 1m. |
| G2.6 isolation | Before/after hashes of production (container ids, image ids, compose config) are equal. 0 writes outside the namespace. |

## 2. Shadow-only fault cases (R-cert 3, part B)

The offline cases are in `tests/test_v230_derived_bars.py` (`FaultMatrixTests`, F1–F9). The cases below need real roles. They run
inside the window, away from the Monday boundary, one at a time.

| Case | Injection | Expected |
|---|---|---|
| S1 | Restart bar_edge between two chain levels, about 10 s after a :00 hour close | Watermark resumes. Re-derived bytes are identical, so the projector counts duplicates and stores no second row. |
| S2 | Stop the ingestor for 3 min | 1m holes appear. bar_edge native gap repair fills them from REST, and the waiting targets publish. If the venue truly has no row, a visible gap is recorded and no bar is published. |
| S3 | Restart the projector | The cache resumes from its checkpoint. Derivation waits on readiness and never derives from a half-built generation (KnBarReadback generation fence). |
| S4 | Flush the shadow Redis cache and let the projector rebuild it | bar_edge rebases on the cache epoch change. No derived bar is built from a partial rebuild. |
| S5 | Replay derived rows into a v2.2.4 projector (prod image `60f83a51`, shadow topic) | Refused with `ENVELOPE_NOT_CANONICAL`. This proves the rollout-order rule: projector, stream, query and core move to v2.3.0 before any bar_edge derives. |
| S6 | Restart the core | No duplicate canonical rows; the derived binding resumes. |

These items belong to the declaration slice's own packet and are not repeated here:
- checkpoint superset/subset;
- JWT accept-set N−k / N+1;
- lane stability.

## 3. Topology

Everything below lives in one disposable namespace `qdl-v230-rcert2-<utc>`. The pattern is that of
`scripts/p184_g12_isolated_fault.py`:
- one holder container;
- every role joins `--network container:<holder>`;
- containers are labelled `qdl.v230.rcert2=<ns>`, named `<ns>-<role>`, and started with `--rm`.

The holder sits on a new user-defined bridge network `<ns>-net`. It has outbound internet for public OKX WS and REST. It publishes
no port and is attached to no production network.

| Role | Image | Hard cap | Expected use |
|---|---|---|---|
| holder | `alpine:3.20` (existing) | 0.05 CPU / 32 MiB | ~0 |
| kafka (1 KRaft node, mTLS, RF1) | `apache/kafka@sha256:9516fb76…` (existing, the production broker image) | 0.35 / 768 MiB (heap 384 MiB) | 0.05–0.15 |
| market_cache (Redis) | `redis@sha256:dfa18828…` (existing) | 0.10 / 256 MiB | <0.02 |
| ingestor (OKX business WS, `candle1m` x 5 only) | v2.3.0 rust image (new) | 0.10 / 128 MiB | <0.02 |
| core (1 worker) | v2.3.0 rust image (new) | 0.15 / 128 MiB | <0.03 |
| projector (stage A+B) | v2.3.0 rust image (new) | 0.15 / 256 MiB | <0.03 |
| bar_edge (bootstrap 300 rows per binding, derive 13 intervals) | v2.3.0 python image (new) | 0.20 / 512 MiB | 0.02–0.05; Monday burst is 65 bars |
| comparator (OKX REST reference + cache readback, writes receipts) | v2.3.0 python image (new) | 0.05 / 256 MiB | <0.01 |
| **Total** | | **1.15 CPU / 2.3 GiB** | **about 0.2–0.35 CPU** |

Topics are created in the shadow broker only, with production names because the binaries expect them:
- `md.raw.realtime.v2`
- `md.canonical.v2`
- `md.quarantine.stable.v1`
- `md.bars.v2`
- the projector's internal topics

Retention is 72 h on every topic.

Declarations come from a 5-instrument acquisition plan rendered by the code under test:
- 1m native `RUST_NATIVE`;
- 13 `DERIVED_FINAL` rows on the chain 3m,5m←1m; 15m←5m; 30m←15m; 1h←30m; 2h←1h; 4h,6h←2h; 12h←6h; 1d←12h; 2d,3d,1w←1d.

The catalog subset is taken from `config/v2/stable-source-bindings.yaml`. These are the declaration tool's rows, generated
for the same 5 instruments.

TLS uses a throwaway CA and leaves made with `scripts/phase80_generate_tls.sh` in the namespace work dir. They are destroyed at
teardown. The production mesh CA and certs are never read.

## 4. Images

Existing images are reused:
- kafka `apache/kafka@sha256:9516fb7634ba…`
- redis `redis@sha256:dfa18828cbc0…`
- `alpine:3.20`
- the v2.2.4 projector `qdl-v2-rust:2.2.4-c7ce7d3` (`60f83a51`), used only for S5

Two images are built from the merged v2.3.0 tree, at commit `<merge-sha>`, recorded before the build:
- `qdl-v2-rust:2.3.0-rc1-<sha8>`:
  - built with `docker build -f Dockerfile.qdl-rust-runtime -t qdl-v2-rust:2.3.0-rc1-<sha8> .`
  - the release build needs `--cpus 2` (the coordinator rule allows 2 for cargo release builds);
  - [estimate] 30–45 min on the current host;
  - builder cache pruned afterwards, by build id.
- `qdl-v2-python:2.3.0-rc1-<sha8>`:
  - built from `Dockerfile` with the verified-dependencies stage of the release pipeline;
  - [estimate] 5–10 min at 1 CPU.

Both digests are written to the packet receipt from `docker inspect`. The same images later become the v2.3.0 release
candidates, so there is no second build.

## 5. Cost against the DL budget

Measured `docker stats` snapshot at 2026-11-13 08:38Z, production DL roles: about 4.9 vCPU in total, of which the three
Kafka brokers take 3.3. This is one sample, not a 60 s average.

| | vCPU |
|---|---|
| Shadow, expected | +0.2 to +0.35 |
| Shadow, hard cap | +1.15 |
| DL with shadow, expected | about 5.1–5.3 (within the owner's 6.0 allowance) |
| Worst case at every cap | about 6.05 (the caps are deliberately tight; observed traffic is about 2 records/s) |

| Other resource | Cost |
|---|---|
| RAM | up to 2.3 GiB capped; [estimate] about 0.9 GiB used |
| Disk, data | [estimate] under 0.5 GB for 72 h (about 2 raw records/s plus canonical, bars and Redis) |
| Disk, images | about 1.2 GB |
| Disk, build cache | transient, pruned |
| Disk, free now | 125 GB |
| OKX REST | Bootstrap about 70 requests. Comparator about 13 per hour-close plus bursts at day and week boundaries; under 0.1 req/s on average. Uses the shared public per-IP budget, paced at 2 req/s or less, with back-off on 50011. |

## 6. What is NOT touched

- The production compose project `qdl_v2_stable_candidate` and every one of its containers, networks, volumes and env.
- Production Kafka (kafka1/2/3), `market_cache`, `stable_redis`, Postgres.
- The mesh CA and certs.
- Consumer manifests and JWTs; the catalog and acquisition in `/kn` mounts.
- The TS writer, alphas and portal.

The only production interaction is read-only `docker inspect`/`ps` hashes before and after (G2.6).

## 7. Run, teardown, rollback

**Start.** Sunday 2026-11-15 between 00:00Z and 20:00Z, so that at least 4 h of steady state precede Monday 00:00Z. Run to at least
Monday 2026-11-16 12:00Z. Faults S1–S6 run Monday 06:00–12:00Z, one at a time, each followed by a 30 min recovery window.

**Driver.** `scripts/v230_rcert2_shadow.py`, to be written. It reuses the G12 helpers (`_base`, KRaft role argv, die/oom event
log, bounded log follow) and the parity rules of `scripts/v230_derived_bar_parity.py`.
- Its offline parts (config render, comparator) get unit tests before the first run.
- It runs as one detached driver (`setsid nohup`) with a deadline of 40 h.

**Teardown.** Done by the driver at the end, on any failure, and on demand with `--teardown <ns>`:
- `docker rm -f` every container with label `qdl.v230.rcert2=<ns>`;
- `docker network rm <ns>-net`;
- delete the namespace work dir (Kafka log dirs, Redis data, throwaway TLS).

Receipts (hashes plus bounded metrics) are kept under `~/.local/state/qdl-v2/v230-rcert2/<ns>/`. The count of removed objects is
recorded. Images stay until the release decision. If v2.3.0 is abandoned, they are removed by digest.

**Rollback.** Nothing in production changes, so rollback equals teardown.

**Stop conditions:**
- Any G2.6 isolation hash differs: stop and tear down.
- Host load over 24 for more than 15 min at start: delay the start.
- DL production role CPU over its cap: stop the shadow first.

## 8. Approval asked

Approve the namespace and caps above, the two image builds from the merged v2.3.0 tree (cargo release at 2 CPU), and the run
window. Approval does not cover any production rollout of v2.3.0; that is a separate window and packet.

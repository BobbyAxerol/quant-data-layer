# KN single points of failure - dependency map, recovery, and stated limits

Status: documentation of the current production topology (KN, release v2.2.2 certificate
`upgrade/evidence/releases/v2.2.2/certificate.json`). Written 2026-10-07 for the consolidated Data Layer
release scope (P18.4 Phase A). It does not change runtime and does not claim high availability.

## What is single, and why a replica is not a fix

| Role | Instances | Why a second instance is a design change, not a replica count |
|---|---|---|
| `ingestor_binance_usdm` | 1 | `lease_epoch` is a static fencing token fixed at 1, not leader election; the core refuses two bindings sharing a `source_id` (plan "What is actually single", B3) |
| `ingestor_okx_swap` | 1 | same as above |
| `binance_bar_edge` | 1 | owns BAR checkpoints; active/passive (B2) touches checkpoint ownership, the part that can corrupt rather than stop |
| `stable_redis` | 1 | deliberately ephemeral (`--appendonly no`); holds lease and quota keys in the stable runtime wiring (`qdl/runtime/stable.py` quota wiring). Its exact role set in KN mode is **not verified** here |

All of them share one host and, for the producers, one Kafka cluster. Running a second copy on the same host
does not remove the host or Kafka as a common failure, so even B2/B3 would not make this independently highly
available.

## Dependency map (data path)

```
venue WS/REST -> ingestor_{binance_usdm,okx_swap} -> Kafka raw -> rust_core x3 -> Kafka canonical
              -> market_projector x2 -> market_cache -> query_kn x2 / stream_kn x2 -> consumers (TS, alpha)
venue REST    -> binance_bar_edge -> BAR history (closed bars, > 1m intervals)
stable_redis  -> leases and per-consumer request quota (query/stream admission)
```

## Failure effect and recovery

| Role down | Consumer-visible effect | Recovery today | Measured |
|---|---|---|---|
| one ingestor | its venue's TRADE/QUOTE/BOOK/MARK stop advancing; consumers see quality STALE / not execution-eligible (fail closed), never fabricated data | container restart policy `unless-stopped`; the ingestor re-subscribes and the core resyncs books (gap -> quarantine -> snapshot) | not measured end to end (outage -> first execution-eligible view) |
| `binance_bar_edge` | intervals above 1m stop gaining closed bars; 1m bars are unaffected (since R1.28); longer intervals tolerate 540 s+ | restart policy; checkpoints resume | restart 18 s (plan, B2 note) |
| `stable_redis` | admission depends on it: quota consume fails closed with 503 `DEPENDENCY_UNAVAILABLE` (`qdl/security/data_plane.py` RedisMinuteQuota) | restart; quota counters restart empty (one-minute windows) | not measured |
| `market_cache` (KN) | reads fall back to recovery until rebuilt | rebuilt from Kafka state topics without offset reset or flush (plan, KN incident record) | not measured as a drill |

## Commands (no new tooling)

- Status: `docker ps --filter name=qdl_v2_stable_candidate` and the per-role healthchecks (B1).
- Restart one role: `docker start <container>` - the KN containers were created from private packets, not
  this repository's compose files, so never `docker compose up` (CLAUDE.md section 1).
- Resource and liveness watch: `scripts/kn_production_watch.py`.

## Missing (recorded, not built here)

1. Recovery-time measurement per role (outage -> first execution-eligible consumer view), across both query
   replicas - needed before any availability statement.
2. B3 Rust ingestor HA: a lease client and a fenced takeover in the core (the `lease_epoch` change).
3. B2 bar-edge active/passive, reusing `ActivePassiveGatewayLease` (`qdl/runtime/lease.py`).
4. B4 Redis redundancy: a decision about where lease/quota identity lives, not a replica count.
5. A KN boot runbook script; `scripts/v2_stable_boot_recovery.py` and
   `scripts/rebuild_v2_stable_projection_cache.py` target the retired SQLite-spool topology.

No new service is added for any of these (owner rule: extend existing services, reclaim wrong-direction ones).

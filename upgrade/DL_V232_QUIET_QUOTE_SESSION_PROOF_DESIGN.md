# Design note: quiet QUOTE session proof (DL + TS class fix)

- **Status:** proposal, not implemented. It needs an owner decision.
- **Date:** 2026-11-17.
- **Author:** Claude (DL lane).
- **Trigger:** OKX `BAND-USDT-SWAP` QUOTE was refused intermittently with `EXECUTION_CONTEXT_STALE` (event age
  47-63 s) from 04:15Z.
- **Plan anchor:** `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`, entry "DL v2.3.2 benchmark: snapshot_batch measures
  QUOTE probes only".

## 1. Mechanism (as of TS `origin/dev`, 2026-11-17)

1. **OKX bbo-tbt is delivered on change.** An illiquid swap whose best bid and ask do not move sends nothing. The
   DL marks such a record `DELIVERY_ON_CHANGE` + `LAST_EVENT_STALE`, keeps it `execution_eligible` while the provider
   session is LIVE, and reports `provider_session_liveness_ms`.
2. **The writer stamps a per-record session proof** on every Query QUOTE read:
   - `ts_session_heard_at_ms = receipt - provider_session_liveness_ms`;
   - `ts_session_budget_ms = requirement.max_session_liveness_ms`.
   - Code: `adapters/market_data/data_layer_v2.py:910-923`; Rust writer `rust/crates/market-data-writer/src/view.rs:926-929`.
3. **The budget is 2000 ms.** The TS r14 manifest gives every QUOTE row (102) `max_session_liveness_ms: 2000`,
   `max_freshness_ms: 2000` and `event_recency_policy: OBSERVE`. The writer env has
   `DATA_LAYER_V2_QUOTE_MAX_SESSION_LIVENESS_MS=2000`.
4. **The writer refreshes the proof by probing.** Each OBSERVE QUOTE route is probed every budget / 3 = **666 ms**
   (`rust/crates/market-data-writer/src/session.rs:374-389`, `probe_cadence_ms`). That is about 3,000 Query
   requests per minute (DL v2.3.1 B4 measurement).
5. **Risk accepts an old QUOTE only on a fresh proof.** `_quiet_quote_at_use` requires execution-eligible,
   `DELIVERY_ON_CHANGE`, a LIVE session, and `now - ts_session_heard_at_ms <= ts_session_budget_ms`
   (`services/risk_engine/execution_market_context.py:333-348`). Otherwise it falls back to the strict check, event
   age against the order budget, and raises `EXECUTION_CONTEXT_STALE` (lines 399-419). The reported age (47-63 s)
   is the time since the last BBO change, not a delivery delay.

The margin is three probes. A probe that is late by more than about 1.3 s, on top of up to about 1 s of DL session
liveness, lapses the proof. Late probes come from Query latency, `RATE_LIMITED`, native worker restarts (for example
07:36-07:42Z during the Query x2 R2 recreates) and coalescing in-flight caps. A lapsed proof refuses any order that
needs the QUOTE. A cached record written by the stream path carries no proof fields at all until the next probe
renews it.

## 2. Data (read only: Redis `redis_marketdata` db1 GET/SCAN, no container)

| Sample | Result |
|---|---|
| BAND-USDT-SWAP, 38 samples at 1 s (about 07:43Z) | event age up to **56.1 s**; proof older than 2 s in **8/38** samples (max 9.4 s); DL liveness at most 1,007 ms |
| IOST-USDT-SWAP, 33 samples at 1 s | event age up to 26.4 s; proof older than 2 s in 0/33 |
| All 96 crossec OKX QUOTE keys, 12 sweeps, 4 s apart | proof older than 2 s at least once on 5 symbols (AAVE, AXS, GALA, GAS, SUSHI); 2 more (APE, DOGE) had event age over 2 s with no proof fields on the record; per-symbol max event age p50 4.2 s, p90 15.9 s, max 31.2 s |
| "Before" latency run, 3 × 246 keys (02:50Z) | OKX `execution_quote` freshness at write p95 8.3 s; age at read max 58.7 s |

At any minute about 5-10 % of the 96 are exposed. Illiquid names such as BAND are exposed about 20 % of the time.
The venue is not at fault: it is correct for it to push nothing while the BBO is unchanged.

## 3. Options

**(a) Preferred: a DL Stream per-session "session alive" frame, with the writer stamping it.**
- DL: for each subscribed provider session (OKX lane), Stream emits a typed control frame at least every 500 ms. The
  frame carries `session_id`, `provider_session_liveness_ms`, the DL `session_heard_at_ns` and the signed watermark.
  It is additive and versioned: a new `SubscribeResponse` control kind, and old clients ignore it.
- TS writer: on each frame, re-stamp `ts_session_heard_at_ms` on every cached QUOTE of that session, whether or not
  the BBO changed. This includes records written by the stream path.
- Effect:
  - The proof follows the live session at the frame cadence, not a per-route Query probe.
  - The 666 ms QUOTE probes (about 3,000 requests/min), and their coupling to Query latency, quota and recreates,
    can be retired. B4 `snapshot:batch` remains the fallback.
  - Risk keeps its 2 s rule unchanged.
- Owners: DL (Stream frame and SDK) and TS (writer stamping). Rollout: DL first (additive), then the writer.

**(b) Interim, TS only.**
- Stamp the proof on stream-path writes as well, which closes the records without proof.
- Optionally, by owner decision, give the session proof a window separate from the event freshness. For example,
  `ts_session_budget_ms` 5 s, with event freshness unchanged.
- This narrows the failure window but keeps the probe dependency.

**Not recommended:** raising the 2 s event budget. That would accept genuinely dead quotes.

## 4. Acceptance for whichever option is chosen

- Over 30 min across the 96 crossec OKX QUOTE keys, the fraction of 1 s samples with proof age over 2 s is 0 while
  the provider session is LIVE.
- Writer Query QUOTE probes per minute drop. This applies to (a) only.
- No `EXECUTION_CONTEXT_STALE` on QUOTE while DL reports session LIVE and execution eligible.

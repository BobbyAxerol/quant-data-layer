# DNSE Production Provider Edge Runbook

## Purpose And Authority

This runbook closes the operational boundary for DNSE as the primary Vietnam-
market provider. It implements the architecture in
`upgrade/quant-data-layer-fund-grade-upgrade-architecture.md` section 14.4 and
is tracked by `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md` B.2-D.

DNSE is a Python vendor acquisition edge only. It publishes authenticated raw
TRADE and final BAR envelopes to the private Kafka-compatible raw topic. The
shared Rust core remains responsible for canonicalization, units, ordering,
deduplication, quality and durable publication. Redis and public APIs are never
written by the acquisition edge.

## Security Invariants

- Never use credentials embedded in examples or an unlicensed SDK snapshot.
- Provision a dedicated market-data key with the least privileges DNSE permits.
- Store the key only in the workload secret store; do not place it in Git,
  evidence, command history, Docker labels or compose-rendered artifacts.
- TLS and hostname verification remain mandatory. `CERT_NONE` is forbidden.
- `DNSE_REST_USE_ENV_PROXY=false` is the default. Enabling it requires an
  approved monitored proxy; it does not disable TLS verification.
- The edge receives only DNSE market-data credentials and Kafka producer mTLS
  material scoped to `md.raw.stable.v1`. It receives no Trading System, Redis,
  query, portfolio or broker-order credential.

## Data Paths

```text
cold start / bounded repair:
DNSE versioned REST /price/ohlc
  -> strict pagination/schema/OHLC validation
  -> authenticated raw REST_HISTORY envelope

live:
DNSE authenticated WebSocket tick.G1/tick.G3 + ohlc_closed.1
  -> bounded vendor dispatch queues
  -> bounded lossless edge queue
  -> authenticated raw SDK_CALLBACK envelope

both:
raw Kafka ACK -> atomic DNSE BAR checkpoint
              -> Rust canonical/quality core
              -> canonical Kafka -> stable Python projector/query/stream
```

REST is never polled once per minute. A matching complete checkpoint avoids
cold REST bootstrap after restart. The checkpoint is bound to slice, authority,
catalog revision, acquisition revision and all DNSE BAR binding IDs; any
corruption, partial state or mismatch fails closed.

## Preflight

Run preflight from the intended acquisition host without printing credentials:

1. Confirm `DNSE_API_KEY` and `DNSE_API_SECRET_KEY` are non-empty by boolean and
   length only.
2. Confirm direct TCP/TLS reachability to `openapi.dnse.com.vn:443` and
   `ws-openapi.dnse.com.vn:443` with bounded connect timeouts.
3. Authenticate one read-only WebSocket session and subscribe to
   `ohlc_closed.1` for one approved instrument. Reject any auth error.
4. Call one bounded historical window with `version: 2026-07-23`; require valid
   parallel OHLC arrays, monotonic pagination and a final closed row.
5. Confirm system UTC/NTP health and the `Asia/Ho_Chi_Minh` trading calendar.

A process-up or socket-connect result alone is not acceptance. REST, auth,
subscription, provider bytes and freshness must all pass.

## Egress-Capable Edge Option

If the primary Data Layer host cannot reach official DNSE REST, deploy only
`vn_edge_v2` in an approved independent egress domain:

- outbound allowlist: official DNSE REST/WebSocket and the private Kafka
  endpoints only;
- inbound: none except supervised administration;
- Kafka: mTLS, ACL limited to the raw topic, idempotent producer and `acks=all`;
- state: encrypted durable volume containing only the atomic edge checkpoint;
- identity: unique producer/client ID and source session/generation;
- observability: auth/reconnect, queue depth, ACK latency, last TRADE/BAR,
  checkpoint age and fatal-fence alerts without payload/credential logging.

Do not SSH-tunnel a long-lived production feed as an implicit architecture.
A temporary tunnel may certify reachability only. Do not relabel vnstock or
legacy Parquet as `DNSE_DIRECT`; fallback keeps its own provider identity and
requires an audited source-policy transition.

## Promotion Gates

Promotion requires all of the following:

- exactly 500 authentic final 1m warmup rows per approved DNSE BAR binding;
- native closed-BAR callback observed and durably ACKed;
- restart restores the exact checkpoint and performs no repeated cold bootstrap;
- zero unexplained gap, duplicate, conflict, quarantine or queue drop;
- Python/Rust golden parity for DNSE TRADE/BAR, exact units and VN calendar;
- signed consumer warmup -> replay -> live, freshness/session semantics and
  unchanged V1 consumers;
- explicit operator approval naming image SHA, host, service, topic ACL,
  credential secret, state volume, affected consumers and rollback command.

## Failure And Rollback

Auth rejection, REST exhaustion, malformed data, incomplete history, queue
pressure, missing Kafka ACK, checkpoint mismatch or canonical conflict fences the
edge. It must not publish synthetic repair or advance the checkpoint.

Rollback stops only the isolated DNSE V2 edge and returns its slice to the
retained `2.0.0-2412572` artifacts. V1 remains authoritative until a separately
approved cutover. Preserve bounded hashes/offsets/metrics for diagnosis; remove
all disposable smoke containers and test credentials.

## Current Host Evidence (2026-08-20)

- Official DNSE REST TCP/443 timed out before TLS negotiation.
- Official production WebSocket TCP/TLS connected, but the key currently loaded
  from host `.env` was rejected as invalid before subscription.
- The running V1 process still reports an older authenticated DNSE session and
  `MARKET_CLOSED`; it has zero queue drops. Restarting V1 before credential
  rotation is therefore unsafe.

These are external infrastructure/credential gates, not permission to weaken
provider provenance or claim DNSE production certification.

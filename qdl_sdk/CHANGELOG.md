# SDK Changelog

## 2.11.1 (DL-DEF-2026-11-27-O: request timeouts follow the declared server deadline)

Fix, no API change. `RestQueryTransport.reference_batch` now waits for its requirements' own `deadline_ms` (the
largest in the batch) plus `COLD_TIMEOUT_MARGIN_S` (2 s) whenever that is longer than the transport's
`timeout_seconds`, as `warmup` and `warmup_batch` already did since 2.8.0; one helper now serves all three. Before,
a reference batch declaring `deadline_ms=120000` was abandoned at the transport default (10 s) while the server was
still answering (Crossec 2026-11-27 09:35:20Z: ReadTimeout at 10.06 s, server 200 at 10.69 s). A request whose
deadline is inside the transport default is byte-identical. Callers that used a short transport timeout as a cap
must now set `deadline_ms` to the bound they want (the default `deadline_ms` is 20,000). Snapshot, feed status and
instrument reads declare no deadline and keep the transport default. Replica hedging: see README "Replicas".

## 2.11.0 (Data Layer v2.4.4, DL-BL-2026-11-26-K: instrument lifecycle observations)

Additive only. New `AsyncDataLayerClient.instrument_lifecycle(identity, consumer_grade=..., after_sequence=0,
limit=100)` and the transport method of the same name read `GET /v2/instruments/{identity}/lifecycle` into
`InstrumentLifecycleResponse` (schema `qdl.instrument-lifecycle.v1`): `history_state` (RECORDED, NO_OBSERVATION,
NOT_CONFIGURED), `current` (status, venue_state, since_event_id, since_observed_utc, effective_utc) and the
append-only events (`qdl.instrument-lifecycle-event.v1`: event_id = sha256 of the canonical event, per-instrument
`sequence` and `previous_event_id` chain, venue_state, venue-stated `effective_utc` only, `observed_utc`,
`evidence` with the listing endpoint, venue doc URL and capture sha256). Retired instruments resolve. `status` and
`history_state` are strings so a later value does not break a reader; treat an unknown value as "no lifecycle claim".
Older servers answer 404 for the path; nothing else changes.

## 2.10.1 (DL-DEF-2026-11-25-E: stream BAR lineage)

Fix: `market_data_view_from_stream` now emits the BAR `resample_lineage` from the canonical `Bar.aggregation`,
field for field as Query does. Before this fix, a live AGGREGATED bar had `resample_lineage=None` while its
warmup copy carried it. Contract text: lineage is optional evidence, not revision identity (README, "BAR revision
identity"). No new fields and no change for non-AGGREGATED bars.

## 2.10.0 (Data Layer v2.4.1: canonical instrument metadata v1)

Additive only. `InstrumentView` (and `InstrumentResponse`, page items) gains an optional `canonical`
(`CanonicalInstrumentMetadataView`, schema `qdl.instrument.canonical-metadata.v1`): `status` VERIFIED or UNVERIFIED,
`rule_id`, `underlying_asset`, `underlying_instrument_uid`, `price_multiplier`, `quantity_multiplier`, `aliases`,
`source` (endpoint, venue doc URL, capture time and sha256). Prices, quantities and bars stay venue-native. New
`qdl_sdk.instruments`: `to_underlying_price`, `to_underlying_quantity`, `same_underlying` (exact `Decimal`). They
raise `CanonicalMetadataUnavailable` (codes `CANONICAL_METADATA_MISSING` / `_UNVERIFIED` / `_INCOMPLETE`) and never
assume 1:1. Older servers simply omit `canonical`; older SDKs ignore it.

## 2.9.0 (Data Layer v2.4.0 A-FUND: historical 1m MARK/INDEX candles)

Additive only. `ReferenceRequirement` for `MARK_INDEX_PRICE` now also accepts a bounded window at a non-execution
grade: `interval="1m"`, `mark_index_kind` MARK or INDEX (not BOTH), `start_time_ns`/`end_time_ns` on whole minutes,
`limit` 1..100. The Data Layer answers with one observation per 1m candle (`observed_at_ns` = candle open; fields
`mark_open/high/low/close` or `index_*`; labels `interval`, `price_type`, `candle_close_ns`, `confirm`), e.g. the
mark price at a past funding time: window `[floor_1m(T), floor_1m(T) + 60 s)`, `limit=1`. OKX SWAP/FUTURES only
(Binance answers UNAVAILABLE). Unchanged: the snapshot shape (no window, no interval) and execution-grade
MARK_INDEX_PRICE, which stays a one-row live snapshot; a 2.8 request serializes byte-identically.

## 2.8.0 (Data Layer v2.4.0: optional session fairness hint)

Additive only. `RestQueryTransport` and `build_replicated_rest_query` gain keyword-only
`session_id` (default None). When set, every Query request carries `X-QDL-Session:
<session_id>` (1..128 printable characters). The Data Layer uses it only for fairness
inside the authenticated identity: the session-fair warmup queue and a per-session share
of the identity's request rate, so one noisy session of a shared tier identity is
throttled alone. It never authenticates, authorizes or widens anything. Default None:
request bytes equal 2.7.0. Server side (no SDK change needed): JWT role claims are
ignored, the token manifest revision is advisory (`min_token_revision: 1`), refusals and
shared-cache hits no longer spend quota, and a queued warmup waits up to its
`deadline_ms` with `retry_after_ms` from its queue position.

## 2.7.0 (Data Layer v2.3.2: session-alive frames for quiet QUOTE/TRADE streams)

Additive only. `GrpcStreamTransport` gains keyword-only `session_alive_interval_ms`
(default 0 = off; otherwise 250..500). When set, `subscribe()` sends the additive
`SubscribeRequest.session_alive_interval_ms = 9` and yields `SessionAliveEvent`
(`StreamRecord.session_alive = 12`, schema `qdl.stream.session-alive.v1`): the
session id, connection generation and config revision of the instrument's last event,
the ingestor's last transport time, the liveness age at emission, the gateway epoch and
high watermark, and the current resume token unchanged. `WarmupStreamSession` passes
the event through without touching offsets or acknowledgements. A refused request maps
to `DataLayerError` code `SESSION_ALIVE_INVALID` / `SESSION_ALIVE_UNAVAILABLE`.

`GrpcStreamTransport` also gains keyword-only `delivery_on_change_flag` (default False).
When True, `subscribe()` sends the additive `SubscribeRequest.delivery_on_change_flag = 10`,
and the gateway marks QUOTE records of an `ON_CHANGE` binding, live and catch-up, with the
new `QualityFlag.QUALITY_FLAG_DELIVERY_ON_CHANGE = 18`. That is the Query rule;
`market_data_view_from_stream` shows it as `"DELIVERY_ON_CHANGE"` in `view.quality.flags`.
It is opt-in because a reader before 2.7.0 refuses an unknown `QualityFlag` value
(`QualityFlag.Name`).

Unchanged for every other client: with the defaults (0, False) the request bytes equal 2.6.0, and
a `session_alive` record a client did not ask for is ignored. Why: a quiet BBO left the
TS writer's session proof older than Risk's 2 s rule while the venue was healthy
(design `upgrade/DL_V232_SESSION_ALIVE_FRAME_DESIGN.md`).

## 2.6.0 (Data Layer v2.3.1: per-request Query replica balancing)

Additive only. New `build_replicated_rest_query(urls, *, tls, credential_provider,
timeout_seconds, max_connections_per_replica=100, max_attempts=2, cooldown_seconds=1.0)`
returns the existing `ReplicatedRestQueryTransport` over one `RestQueryTransport` pool per
Query replica URL, with keep-alive equal to the pool's connection limit.
`RestQueryTransport` gains keyword-only `max_connections` (default 100) and
`max_keepalive_connections` (default 20, unchanged behaviour).

Why: one pool on a DNS alias that names several replicas (`qdl-v2-query`) selects a
replica per TCP connection, and httpx then always reuses the oldest free connection.
A sequential caller, such as a coalesced `reference_batch` stream, therefore stays on one
replica for the life of the process (production 2026-11-17: query_kn_1 0.94 vCPU with about
213 slow requests/min, query_kn_2 0.26 vCPU with none). With httpx 0.28.1/httpcore 1.0.9,
a pool above its keep-alive limit also closes idle connections, so a bursty caller
re-handshakes TLS. Per-replica pools with per-request round-robin remove both effects.
Replica URLs must match the Query certificate SANs (`query_v2_1`, `query_v2_2`).
Clients on 2.5.0 keep working; no wire or model change.

## 2.5.0 (Data Layer v2.3.0: derived-bar lineage in the generated contract)

Additive only; no SDK source change. The bundled generated contract gains
`Bar.aggregation = 19` (`BarAggregation`: base interval and source, constituent count,
first/last constituent event ids and offsets, constituent SHA-256). The field is present
exactly on bars with `BAR_ORIGIN_AGGREGATED` (an existing enum value), i.e. bars the Data Layer
derived from finer final bars (`DERIVED_FINAL`). Older clients ignore it; 2.4.1 keeps
working against v2.3.0. The version moves to 2.5.0 because the wheel's generated contract
(and so its bytes) changed; a 2.4.1 wheel with different bytes would contradict the v2.2.5
release asset.

## 2.4.1 (replay budget status opt-in, progress-bounded reconnects; IDR-096)

`GrpcStreamTransport` sets the additive `SubscribeRequest.replay_budget_status_v2 = 8`
(bool). A stream gateway built from f5cd787 or later (Data Layer v2.2.4) then ends a Subscribe catch-up that reaches a replay
work budget (`REPLAY_MATCHED_LIMIT`, in-scan `REPLAY_SCAN_LIMIT`, `REPLAY_BYTE_LIMIT`)
with retryable `RESOURCE_EXHAUSTED "RATE_LIMITED:{reason}:..."` instead of
`OUT_OF_RANGE "CURSOR_EXPIRED:..."`. An older gateway ignores field 8 and keeps the
`CURSOR_EXPIRED` mapping; both are handled. A range refused at submit (its offset span
alone exceeds the scan budget) stays `OUT_OF_RANGE CURSOR_EXPIRED:REPLAY_SCAN_LIMIT`
for every client. `WarmupStreamSession` renews its reconnect budget only when a
delivered offset exceeds the highest offset this session generation has yielded, so
a retryable replay budget that re-delivers the same unacknowledged window cannot retry
forever. Clients that do not set field 8 (2.1.x-2.4.0) see no change. Public models
and positional `StreamEvent` construction are unchanged.

## 2.4.0 (Subscribe gateway clocks)

`StreamEvent` gains three optional diagnostic fields, `gateway_dispatch_wall_ns`,
`gateway_dequeue_wall_ns` and `gateway_send_wall_ns` (int Unix ns or `None`),
read from the additive protobuf fields `StreamRecord.gateway_dispatch_wall_ns = 3`,
`gateway_dequeue_wall_ns = 4` and `gateway_send_wall_ns = 5`. A 2.4.0 stream
gateway sets them on every Subscribe event record, each record of a batched
response with its own dispatch and dequeue clocks: dispatch = the live hub
queued the record for the subscription (0/`None` for replayed records), dequeue
= the subscription task took it, send = the gRPC transport took the response
from the gateway's bounded handoff to encode it. They split the Data Layer
ingest -> SDK segment into hub backlog, subscription queue wait, transport
pull (client flow control) and network + client buffers. Controls and Replay
records carry none; an older gateway sends none (all `None`). The fields are
not part of `StreamEvent` equality and are never delivery authority;
positional construction `StreamEvent(offset, token, event)` is unchanged, and
2.1.x-2.3.x clients ignore the new wire fields.

## 2.3.0 (batched Subscribe)

`GrpcStreamTransport` asks the stream gateway for up to
`max_records_per_response` records per `SubscribeResponse` (new keyword,
default 64, 1..256; 1 sends the pre-2.3.0 request without the field). The
protobuf contract gains `SubscribeRequest.max_records_per_response = 7` and
`SubscribeResponse.records = 2` (repeated `StreamRecord`); both are additive.
A gateway that knows the field packs only records it already has queued for
a lossless feed (TRADE, BOOK_SNAPSHOT, BOOK_DELTA), never waits to fill a
batch, never puts a control in a batch and clamps the value to 256 records or
1 MiB; latest-state and lifecycle-coalesced feeds keep one record per response.
The transport reads `records` when present and `record` otherwise, so callers
get the identical sequence of per-record `StreamEvent`/`ControlEvent` objects,
each with its own logical offset and resume token, from a batching gateway, a
gateway without the field (one record per response) or with batching off.
`StreamTransport.subscribe`, `WarmupStreamSession` and acknowledgement are
unchanged. 2.1.x/2.2.x clients never set the field and are served exactly as
before by a 2.3.0 gateway.

## 2.2.1 (v2.2.3 Data Layer)

`GrpcStreamTransport` starts on a random target per instance instead of always
the first one, so the stream replicas share the subscriptions; failover on
UNAVAILABLE is unchanged. A new keyword `start_index` pins the start (tests,
diagnostics). Additive; no request or response change.

## 2.2.0 (v2.2.3 Data Layer)

`ReplicatedRestQueryTransport` starts its round-robin on a random replica per
instance instead of always replica 0. A new keyword `start_index` pins the start
(tests, diagnostics). Additive; no request or response change.

Replica redirect for a busy large-warmup lane (P18.4 G5): `ReplicatedRestQueryTransport`
`warmup`/`warmup_batch` send `X-QDL-Replica-Redirect: 1` on the first attempt when another
candidate replica remains. On exactly RATE_LIMITED with `X-QDL-Refusal-Reason:
REPLICA_LANE_BUSY` (Retry-After 0) the same request goes once to the next replica with the
header `0`, which queues there (no ping-pong); every other typed error stays authoritative and
a busy replica is not marked unhealthy. Each attempt is one HTTP request and consumes one unit
of the identity's per-minute quota. `DataLayerError.reason` (optional, default `None`) carries
the response's `X-QDL-Refusal-Reason`; `RestQueryTransport.warmup`/`warmup_batch` take an
optional `replica_redirect` keyword (None = no header, the pre-2.2.0 request). Servers without
the header support, and requests without it (SDK 2.1.0), queue exactly as before.

`MarketDataView.event_id` (optional, default `None`): lowercase hex of the canonical
`EventEnvelope.event_id`, rendered by the v2.2.3 server on Query snapshot/latest/warmup
rows and set by `market_data_view_from_stream` from the delivered envelope, so a
snapshot of an event and its stream delivery carry the same value (IDR-082). Additive
response field; 2.1.0 readers ignore it, existing fields are unchanged.

IDR-040 fix 2: `QualityView.proof_observed_at_ns` and
`ProblemDiagnostics.proof_observed_at_ns` (optional, positive int, BOOK_SNAPSHOT
only; also `OrderBookSnapshot.proof_observed_at_ns = 8` in the market-data
proto). It is the local receipt time of in-band transport evidence that a quiet,
sequence-verified book was still current; it never re-dates `observed_at_ns`
and never changes `freshness_ms`, `state` or `execution_eligible`. The server
emits the key only when a proof exists (absent otherwise), so 2.0.x strict
readers are unaffected until quiet proofs are enabled; 2.1.0+ readers accept it.

Required-batch item diagnostics (P18.4 stage 100g): the `PARTIAL_RESULT`
`DataLayerError` raised by `reference_batch`/`warmup_batch` with `require_all=True` now
sets `diagnostics = {"origin": "sdk_reference_batch" | "sdk_warmup_batch",
"item_problem_count", "item_problems": [...]}`; each item (first 32) carries
`instrument_uid`, `feed`, `code`, `reason_tokens` (typed UPPER_SNAKE tokens of the item
detail), `retryable`, `retry_after_ms`, `request_id` and `detail_sha256` (the digest
Query's `qdl_refusal` item line carries), never the detail text or data. `code`,
`detail`, `retryable` are unchanged; `diagnostics` was `None` on this path before.

## 2.1.0

Response models, including nested market/reference payloads and error diagnostics,
ignore unknown additive fields. Known fields, enum values and domain validators
remain enforced. Request models, signed payloads, manifests and sealed binding
documents remain strict. Existing server responses remain compatible. No new
server response field is introduced by this release. Deploy tolerant readers
before introducing additive response fields; older 2.0.x readers remain strict.

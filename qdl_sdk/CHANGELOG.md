# SDK Changelog

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

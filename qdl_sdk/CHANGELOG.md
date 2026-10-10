# SDK Changelog

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

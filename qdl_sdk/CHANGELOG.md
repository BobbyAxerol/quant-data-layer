# SDK Changelog

## Unreleased

`ReplicatedRestQueryTransport` starts its round-robin on a random replica per
instance instead of always replica 0. A new keyword `start_index` pins the start
(tests, diagnostics). Additive; no request or response change.

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

## 2.1.0

Response models, including nested market/reference payloads and error diagnostics,
ignore unknown additive fields. Known fields, enum values and domain validators
remain enforced. Request models, signed payloads, manifests and sealed binding
documents remain strict. Existing server responses remain compatible. No new
server response field is introduced by this release. Deploy tolerant readers
before introducing additive response fields; older 2.0.x readers remain strict.

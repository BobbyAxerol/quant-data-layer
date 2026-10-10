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

## 2.1.0

Response models, including nested market/reference payloads and error diagnostics,
ignore unknown additive fields. Known fields, enum values and domain validators
remain enforced. Request models, signed payloads, manifests and sealed binding
documents remain strict. Existing server responses remain compatible. No new
server response field is introduced by this release. Deploy tolerant readers
before introducing additive response fields; older 2.0.x readers remain strict.

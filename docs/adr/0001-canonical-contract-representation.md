# ADR-0001: Canonical Contract Representation

- Status: Accepted for Phase 1 dark deployment
- Date: 2026-08-13

## Decision

QDL V2 contracts use Protobuf package majors (`qdl.*.v1` and
`qdl.marketdata.v2`) governed by Buf. Python and Rust types are generated from
the same source and checked against deterministic golden bytes.

Price, quantity and rates use an exact coefficient plus scale. An int64
coefficient is preferred; `mantissa_text` is the overflow-safe representation.
The venue-native decimal spelling is retained for audit. Binary floating point
is prohibited in canonical messages.

## Consequences

Field numbers are never reused. Additive changes within a major require Buf
compatibility checks; semantic or unit changes require a new major. Generated
files are committed but never edited by hand. V1 JSON/Redis payloads are not
changed by this decision.


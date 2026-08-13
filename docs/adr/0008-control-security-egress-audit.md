# ADR 0008: Control Identity, Egress And Immutable Audit

## Status

Accepted for the separated V2 control role on 2026-08-13. The authoritative V1
combined runtime is intentionally unchanged during shadow certification.

## Decision

The separated control role fails closed unless it has a pinned JWT issuer,
audience, key IDs, signed-algorithm allowlist and fsync-backed audit path. JWTs
are short-lived workload identities carrying environment, role and optional
venue scope. RBAC uses explicit permissions; a token from one environment
cannot operate another.

Outbound providers are selected by registered `source_id`. Scheme, exact host,
port and path prefix are allowlisted; private, loopback, link-local, metadata
and unregistered targets are rejected. Payloads are bounded by bytes, nesting,
numeric length and decompression ratio. Security-sensitive fields are redacted
before audit or log serialization.

Mutating control requests append to a sequential SHA-256 hash chain with fsync.
This makes accidental or unauthorized modification detectable. Production may
replace the local sink with an append-only remote audit store behind the same
record contract.

## Consequences

- A V2 control process cannot start with anonymous production defaults.
- Test HS256 keys are allowed only when explicitly configured by isolated tests;
  production policy permits RS256/ES256.
- Existing V1 consumers continue operating until an identity migration has been
  approved. This ADR does not authorize exposing the control listener publicly.
- DNS and network-policy enforcement remain deployment controls in addition to
  application allowlists; neither is treated as a substitute for the other.

## Mapping

This ADR implements the intent of architecture-guide ADR-014.

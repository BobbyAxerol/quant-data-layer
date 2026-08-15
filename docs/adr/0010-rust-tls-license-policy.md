# ADR 0010: Rust TLS Dependency License Policy

## Status

Accepted on 2026-08-13 after Phase 6 supply-chain certification found policy
drift introduced by the Rust WebSocket/TLS dependency graph.

## Decision

The Rust dependency allowlist includes `ISC`, `BSD-3-Clause` and
`CDLA-Permissive-2.0` in addition to the existing MIT/Apache/Unicode licenses.
They are permissive licenses used by the pinned Rust TLS trust stack:

- `ring`, `rustls-webpki` and `untrusted`: ISC or Apache-2.0 AND ISC;
- `subtle`: BSD-3-Clause;
- `webpki-roots`: CDLA-Permissive-2.0.

The change is an explicit license review, not a wildcard exception. Unknown
registries, unknown Git sources, wildcard dependencies, yanked crates and
unlisted licenses remain denied. `cargo-deny` remains a merge/release gate.

## Consequences

- Current `rustls`/WebSocket dependencies can pass a deliberate policy.
- A future dependency with another license still fails closed and requires a new
  review.
- The duplicate `windows-sys` versions remain a warning for non-Linux target
  compatibility and are not a runtime security or licensing blocker.

# ADR 0009: Capability-Scoped Venue Certification

## Status

Accepted for Phase 6 on 2026-08-13.

## Decision

Certification is scoped by provider, market and feed capability. A venue is not
represented by one global healthy flag. Binance USD-M trade, OKX JSON trade,
OKX deep/SBE book, DNSE bar and a future Deribit option book each have separate
gates, owners, evidence and rollback.

Every applicable adapter must prove instrument mapping, native precision,
sequence semantics, reconnect/resubscribe, rate limiting, malformed quarantine,
duplicate/out-of-order/gap handling, canonical schema, quality state, source
authority, rollback, performance and telemetry/runbook readiness. A mandatory
blocked gate makes only that scope ineligible.

OKX JSON is the authoritative implementation baseline. SBE remains capability
gated until exact schema/version and entitlement are pinned, unknown templates
fail closed, JSON shadow parity passes and JSON rollback is rehearsed. DNSE L2
being unsupported does not invalidate DNSE historical bars. Deribit-style
fixtures prove option-domain extensibility only; they cannot certify an actual
Deribit source.

## Consequences

- Unsupported/tier-gated products fail independently from core feeds.
- A synthetic fixture can prove deterministic logic but never production source
  correctness or provider availability.
- New venues implement the common evidence contract without adding central
  provider conditionals.
- Authority is changed only per approved feed slice after a separate operational
  cutover gate.

## Reversal

Set the affected feed authority to the previously certified provider and replay
from its durable cursor. No canonical contract rollback or data deletion occurs.

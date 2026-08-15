# ADR 0007: OpenTelemetry Boundary And Data-Product SLOs

## Status

Accepted for the V2 shadow runtime on 2026-08-13. Production activation still
requires a deployed OpenTelemetry Collector and approved storage backends.

## Decision

QDL uses the field and metric taxonomy in Section 25 of the fund-grade guide as
the vendor-neutral telemetry contract. Python and Rust roles emit through one
collector boundary; they do not configure individual metrics, trace or log
backends directly.

`BoundedTelemetry` is the deterministic in-process boundary and test double.
It rejects unbounded labels, bounds series and histogram memory, and hashes an
instrument into one of 64 metric buckets. Exact instrument, event, cursor and
sequence identity remain correlation fields and lineage attributes, never
metric labels.

SLOs are evaluated per data-product grade. Any canonical drop is SEV-1. A
connected socket does not imply readiness; availability requires authoritative
source, freshness and closed-gap policy. Production authority requires the
collector, dashboards, actionable alerts and retained release evidence.

## Consequences

- The hot path cannot create an unbounded series per event or instrument.
- Operator logs can correlate exact events without placing raw payloads in logs.
- Backend selection remains reversible at the collector.
- Unit-test telemetry is not evidence that production observability is active.
  Certification must mark the infrastructure gate blocked until collector and
  dashboards are deployed and exercised.

## Mapping

This ADR implements the intent of architecture-guide ADR-013.

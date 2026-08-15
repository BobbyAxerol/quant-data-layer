# Architecture Decision Index

The numbered files in this directory record accepted implementation decisions.
The architecture guide Section 40 is a decision-topic index, not a requirement
to pretend undeployed infrastructure has already been accepted.

| Decision topic | Current record |
|---|---|
| Boundary, canonical contract and precision | ADR 0001 |
| Instrument identity and aliasing | ADR 0002 |
| Python/Rust runtime role boundaries | ADR 0003 |
| Durable transport selection inputs | ADR 0004 and ADR 0006 |
| V1 compatibility and migration ownership | ADR 0005 |
| Redis bounded bridge and rebuild role | ADR 0006 and Phase 2 runbook |
| Observability and SLO standard | ADR 0007 |
| Security, egress and audit | ADR 0008 |
| Capability-scoped venue certification | ADR 0009 |
| Rust TLS/license policy | ADR 0010 |
| Historical atomicity and handoff semantics | Phase 4 report and contracts |
| Provider source authority and bar finality | canonical contracts and Phase 4 report |

Kafka-compatible replication, shared Iceberg/object storage, production OTel,
external secrets and regional DR remain production activation decisions. Their
interfaces and fail-closed gates are implemented, but Phase 6 does not label
same-host substitutes as those technologies.

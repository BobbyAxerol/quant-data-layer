# qdl-core

Deterministic, transport-neutral Rust foundation for the QDL V2 shadow path.

Phase 2 deliberately contains no venue sockets and no direct legacy Redis
writer. The crate owns exact decimal conversion, deterministic event IDs,
canonical fixture mapping, protocol simulation, backoff/rate-limit primitives,
connection/session fencing, queue policy, telemetry counters and broker traits.
Production ingestion, broker clients and authority cutover are later gated
phases.

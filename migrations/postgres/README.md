# QDL control-plane migrations

These migrations are additive, forward-only and dark in Phase 1. They store
instrument/control metadata only; tick, trade and order-book event streams are
explicitly excluded from PostgreSQL.

Production application requires an approved backup, an immutable migration
artifact and a maintenance/change record. The Phase 1 rollback is to disable
the new resolver and continue from its exported read-only registry snapshot;
tables are retained for audit instead of being dropped automatically.

Validation runs twice against a clean disposable PostgreSQL instance and once
against an instance containing an unrelated legacy table. No production-like
database or volume is used by the validation script.


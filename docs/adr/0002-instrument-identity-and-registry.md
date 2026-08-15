# ADR-0002: Instrument Identity And Registry

- Status: Accepted for Phase 1 dark deployment
- Date: 2026-08-13

## Decision

An instrument has an immutable UUIDv5 `instrument_uid` derived from a stable,
human-readable `instrument_id`:

```text
{VENUE}.{MARKET}.{PRODUCT_TYPE}.{CANONICAL_SYMBOL}
```

Provider aliases are temporal and point to a specific instrument metadata
revision. Venue, provider and source instance remain independent concepts.
Unknown allowlist symbols become discovery requirements; hot paths never invent
tick size, product type or contract metadata.

OKX tradable identities are populated from `/api/v5/public/instruments`.
`instId`, `instFamily`, expiry, strike, option type and multiplier are preserved
from registry records; derivatives are never constructed with string guesses.

## Consequences

Spot, perpetual, dated future, VN derivative and option identities cannot
collide. The registry can export a read-only snapshot so data-plane reads can
continue during a control DB outage. PostgreSQL stores metadata/control state,
not market ticks.


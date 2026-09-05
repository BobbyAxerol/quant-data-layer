# QDL Python SDK V2

`qdl_sdk` is the provider-neutral correctness boundary for Quant Data Layer V2.
Phase 7 hardens it as a protected beta; V1 remains authoritative until a
consumer manifest is explicitly accepted and activated.

Every call uses a short-lived workload JWT through `CredentialProvider`. The
token subject, environment and `consumer_manifest_revision` must match the
registered consumer manifest. REST requires both Bearer authentication and
`X-QDL-Consumer-ID`; gRPC sends the same identity and purpose as call metadata.
The server intersects JWT role scope with manifest permissions, requirements
and quotas rather than trusting request-controlled grade/source fields.

## Startup and recovery

1. Load an audited `DataRequirement` manifest.
2. Call REST warmup or snapshot and validate identity, coverage, source quality,
   final-bar policy and execution eligibility.
3. Subscribe with the opaque signed cursor returned by that response.
4. Observe typed `REPLAYING` and `LIVE` controls, then apply events in strict
   logical-offset order.
5. Persist a cursor only after consumer state is durably applied by calling
   `session.acknowledge(event)`. The first acknowledgement after a fresh
   snapshot atomically establishes a new local offset baseline; later
   acknowledgements remain strictly monotonic.
6. On cursor expiry, rebuild from the supplied fresh snapshot after receiving
   `SNAPSHOT_REPLACED`. On a retryable disconnect, resume from the last
   acknowledged cursor.

By default a process restart rebuilds state from a fresh snapshot and ignores an
old checkpoint. Set `resume_restored_state=True` only when the consumer has
atomically restored the local state associated with that checkpoint.

```python
from qdl_sdk import DataRequirement, Feed, Grade

instrument = await client.resolve_instrument(
    venue="BINANCE",
    market="USDM",
    product_type="PERPETUAL",
    native_symbol="BTCUSDT",
    consumer_grade=Grade.EXECUTION,
)
requirement = DataRequirement(
    instrument_uid=instrument.instrument_uid,
    feed=Feed.TRADE,
    consumer_grade=Grade.EXECUTION,
    source_policy_id="crypto_primary_v2",
    max_freshness_ms=1000,
)

snapshot = await client.snapshot(requirement)
exact_price = snapshot.data.payload.price
# exact_price.coefficient / exact_price.scale; no binary-float conversion

async with client.warmup_then_stream(
    requirement,
) as session:
    async for item in session:
        if hasattr(item, "event"):
            persist_state(item.event)
            session.acknowledge(item)
        else:
            handle_control(item)
```

## Universal BAR warmup and closed-bar handoff

BAR consumers declare either an exact row count or an exact half-open time
range. They also declare whether the service may resample a non-native interval
from complete final constituents. The server never silently changes the
instrument, interval, horizon or source policy.

```python
from qdl_sdk import (
    ClosedBarHandoff,
    DataRequirement,
    Feed,
    Grade,
    IntervalSourcePolicy,
    RecoveryPolicy,
    WarmupSpecification,
)

requirement = DataRequirement(
    instrument_uid=instrument.instrument_uid,
    feed=Feed.BAR,
    consumer_grade=Grade.ALPHA,
    source_policy_id="crypto_primary_v2",
    interval="15m",
    recovery=RecoveryPolicy.FRESH_SNAPSHOT,
    warmup=WarmupSpecification(
        rows=700,
        interval_source_policy=(
            IntervalSourcePolicy.NATIVE_OR_EXACT_RESAMPLE
        ),
        max_cache_age_ms=60_000,
        deadline_ms=20_000,
    ),
)

warmup = await client.warmup(requirement)
handoff = ClosedBarHandoff.from_warmup(
    warmup,
    interval="15m",
    maxlen=700,
)

# `latest_closed_bar` comes from the same typed V2 BAR contract. A new final
# bar is appended FIFO before the strategy callback runs, with no extra-bar lag.
await handoff.append_closed(latest_closed_bar, release=recompute_strategy)
```

For a portfolio or universe, call `client.warmup_batch(requirements)`. The SDK
chunks transport requests without serializing every symbol, preserves input
order and cardinality, and validates every item. `require_all=True` is the
default and mandatory for execution-grade requirements; one missing, stale,
gapped or partial item fails the aggregate request. Alpha-grade diagnostic
callers may set `require_all=False`, but must inspect each typed problem and may
not treat the partial batch as execution-ready.

`ClosedBarHandoff` keeps exactly the newest `maxlen` final/revised bars. A
duplicate bar is ignored, a higher revision replaces the same boundary without
re-running the strategy, and a gap or late bar fails closed so the caller can
recover history before continuing.

## Migration safety

- Existing V1 methods remain delegated by `V1CompatibilityFacade` without
  changing their default semantics.
- `REGISTERED` and `ROLLED_BACK` route to V1; `SHADOW` and `ACCEPTED` keep V1
  authoritative while V2 observes; only `ACTIVE` selects V2 authority.
- Insecure gRPC is rejected except for an explicitly enabled loopback test.
- Snapshot and warmup calls return closed typed response models. Unknown fields,
  feed-discriminator mismatches and `UNSPECIFIED` contract enums fail closed.
- The SDK never invents a snapshot ID or stream cursor. Missing server-issued
  handoff metadata is a hard continuity error.
- The SDK never parses cursor internals and never silently accepts stale,
  gapped, partial or non-authoritative execution data.

## Immutable consumer artifact

Build the standalone artifact with:

```bash
python scripts/build_qdl_sdk_release.py --output-dir dist/qdl-sdk
```

The output contains a reproducible `qdl_sdk-2.0.1-py3-none-any.whl`, a release
manifest with the wheel/source/generated-contract SHA-256 digests, and a
CycloneDX SBOM. The wheel contains only the public SDK plus generated Protobuf
contracts; it does not package `qdl.api_v2`, runtime adapters, provider code or
other Data Layer service internals. Trading System and the shared alpha runtime
must pin the same verified wheel digest.

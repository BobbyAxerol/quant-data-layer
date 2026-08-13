# QDL Python SDK V2

`qdl_sdk` is the provider-neutral correctness boundary for Quant Data Layer V2.
Phase 5 certifies it in shadow mode; V1 remains authoritative until a consumer
manifest is explicitly accepted and activated.

## Startup and recovery

1. Load an audited `DataRequirement` manifest.
2. Call REST warmup or snapshot and validate identity, coverage, source quality,
   final-bar policy and execution eligibility.
3. Subscribe with the opaque signed cursor returned by that response.
4. Observe typed `REPLAYING` and `LIVE` controls, then apply events in strict
   logical-offset order.
5. Persist a cursor only after consumer state is durably applied by calling
   `session.acknowledge(event)`.
6. On cursor expiry, rebuild from the supplied fresh snapshot after receiving
   `SNAPSHOT_REPLACED`. On a retryable disconnect, resume from the last
   acknowledged cursor.

By default a process restart rebuilds state from a fresh snapshot and ignores an
old checkpoint. Set `resume_restored_state=True` only when the consumer has
atomically restored the local state associated with that checkpoint.

```python
from qdl_sdk import AsyncDataLayerClient, DataRequirement

requirement = DataRequirement(
    instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
    feed="TRADE",
    consumer_grade="EXECUTION",
    source_policy_id="execution_binance_usdm_v1",
    max_freshness_ms=1000,
)

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

## Migration safety

- Existing V1 methods remain delegated by `V1CompatibilityFacade` without
  changing their default semantics.
- `REGISTERED` and `ROLLED_BACK` route to V1; `SHADOW` and `ACCEPTED` keep V1
  authoritative while V2 observes; only `ACTIVE` selects V2 authority.
- Insecure gRPC is rejected except for an explicitly enabled loopback test.
- The SDK never parses cursor internals and never silently accepts stale,
  gapped, partial or non-authoritative execution data.

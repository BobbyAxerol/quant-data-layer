# External Consumer Endpoint Benchmark

Plan: [Reusable benchmark task](../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#consumer-endpoint-benchmark-20260922).
Tool: [benchmark_consumer_endpoints.py](../../scripts/benchmark_consumer_endpoints.py).
Profile template: [consumer-endpoint-benchmark.json](../../config/examples/consumer-endpoint-benchmark.json).

## Current Safety Finding

**2026-09-22: the first real run exposed query OOM on the global gap diagnostic.**
Both `/v2/data-quality/gaps` calls timed out; kernel logs confirm the respective
query processes were OOM-killed and automatically restarted. The implementation
scans/parses spool records for every catalog binding synchronously. A read-only
endpoint is not necessarily cheap or operationally harmless.

The runner now marks this operation `SAFETY_BLOCKED` and does not call it, even
if selected in an operation filter. There is no bypass flag. This must be
repaired/verified server-side before lifting the guard. All-operation results
remain `INCOMPLETE`, not a green all-endpoint certificate. This task does not
silently modify the production endpoint, raise memory limits or roll services.
Original failed evidence remains available; successful samples during that
incident are not a steady-state baseline. No additional live measurements were
run after the OOM cause was confirmed.

## Run

The host needs Python 3 and access to Docker. Use an existing immutable Data
Layer Python image containing the released SDK. The launcher resolves its
digest locally; it never builds or pulls an image. Store the profile outside
Git. It contains file paths and public JWT metadata, never secret values.

```bash
python3 -B /home/bobby/data_layer/scripts/benchmark_consumer_endpoints.py inventory \
  --profile /home/bobby/.local/state/qdl-endpoint-benchmark/profile.json \
  --output /home/bobby/.local/state/qdl-endpoint-benchmark/inventory-$(date -u +%Y%m%dT%H%M%SZ)

python3 -B /home/bobby/data_layer/scripts/benchmark_consumer_endpoints.py run \
  --profile /home/bobby/.local/state/qdl-endpoint-benchmark/profile.json \
  --output /home/bobby/.local/state/qdl-endpoint-benchmark/run-$(date -u +%Y%m%dT%H%M%SZ)
```

`inventory` uses `--network none`, needs no private-file mounts and performs
no provider/service calls. `run` starts one disposable consumer container per
selected identity, sequentially. Results are `report.json`, `report.csv` and
`report.md` in the new output directory. Exit 1 means failure/incomplete evidence;
an HTTP 200 with invalid data does not count as successful usable latency.

Select identities already present in `stable-v2-release-routing.yaml`:
`trading-system.paper.stable`, `alpha.binance.paper.stable`,
`alpha.okx.paper.stable`, `monitoring.multivenue.stable`. Each needs its own
registered TLS/JWT files and public key ID; never substitute another identity.
Subject, environment, revision, requirements, policy and quotas come from the
exact versioned manifest. A missing V2 scope or mismatched release fails closed.

## Coverage

Current TS scope: **30 products per venue / 60 total**, not 60 different HTTP
URLs. Current four-consumer release scope: **299 products**. The tool enumerates
all selected V2 products; there is no two-symbol/per-feed sample limit.

| Operation | Product use / measurement |
|---|---|
| GET snapshot | Every canonical or pass-through product: BAR, TRADE, QUOTE, L2, etc. |
| GET warmup | BAR with the manifest's exact warmup window |
| GET history | Same governed BAR requirement via the distinct history URL |
| POST warmup:batch | BAR batches; wall time of the whole batch, not divided by items |
| POST reference:batch | Every on-demand product individually, plus batches: funding, OI, long/short, taker, mark/index, basis, metadata |
| GET feed status | Every non-on-demand product; diagnostic validity, not price eligibility |
| GET instruments | One bounded page (500); explicitly reports whether another page exists |
| GET instrument | Every distinct demanded instrument UID; venue/native identity validated |
| GET readiness | Consumer-authorized runtime diagnostic |
| POST readiness:check | Exact requirements, typed per-item status and strict batch shape |
| GET gaps | Inventoried; currently SAFETY_BLOCKED due to the global spool-scan OOM |
| gRPC Subscribe | Optional every durable binding: signed handoff, first validated event and cursor acknowledgment |

The 11 REST operations are checked against the frozen public OpenAPI. A new
unhandled operation fails coverage validation. Legacy V1, internal producer,
admin/write endpoints and orders are explicitly excluded. No fallback, direct
venue connection or authority mutation is performed. Reference reads may warm
ordinary server caches. A provider endpoint's coverage is not a websocket
subscription, and a snapshot is not a full historical archive.

## Limits And Streams

Default: 3 rounds, 1 request/s, batch 8, one in-flight request, 30s timeout,
1800s overall per identity, streams disabled. Pacing uses the smaller of the
profile rate and 10% of manifest request quota. It does not reserve spare quota:
a busy real consumer can still cause a reported `RATE_LIMITED` result. Quotas
and policies are not increased for the test. Batches are capped by the manifest.
The SDK's existing single idle-connection retry remains; the harness does not
retry failed cases until they pass.

Set `stream_seconds` to e.g. `3` for a bounded first-event test on every durable
binding. This is **not** a full reconnect/fallback/C2 certification. It measures
warmup/query + gRPC handoff + wait for the first validated event, not just wire
delivery. A quiet long-interval BAR may legitimately have no new event. The
report records `NO_EVENT`, not a zero-latency success; overall evidence is then
incomplete. Max one test stream is open; no forced disconnect of the provider
or production consumers. Signed cursors remain only in ephemeral client memory.

For an explicitly scoped diagnostic run, the profile may contain e.g.
`"operations": ["reference_batch"]`. Every other operation remains in the
report as `NOT_MEASURED_OPERATION_FILTER`. Omit this field for all operations.
Unfinished cases at the overall deadline remain `NOT_MEASURED_DEADLINE`.

## Reading Results Honestly

- `response_ms`: caller starts SDK transport -> HTTP body received/decoded.
  Includes connection/TLS when opened, signing and network/server work.
- `usable_ms`: same starting point -> SDK schema + exact product/domain quality
  validation completed. It excludes strategy calculation and order submission.
  Diagnostic operations use `meaning=diagnostic_validated`: a valid STALE
  status can return quickly, without making its price executable.
- `elapsed_ms`: includes failed calls; failed samples are excluded from usable
  percentiles but retain typed error code and bounded per-item diagnostics.
- Quality/source event age, session liveness, gap, completeness, execution
  eligibility and policy hash are separate from request latency. Original
  timestamps are preserved. No prices, key material, signed cursor or bearer
  token is written to evidence.
- First request on each replica and round-zero vs subsequent reads are labelled.
  This does not evict server caches: "first" is client-side, not provider cold
  storage. Whole-batch latency is labelled `batch_wall` and never presented as
  individual item latency.
- Per-binding samples are kept, not just a feed average. Small-sample p95 is
  descriptive, not an SLA; p99 is absent below 100 samples **for that case**.
- `PASS_SELECTED_READS` certifies only measured read validity in that run.
  Check explicit exclusions, diagnostic availability and sample counts. It is
  not a release certificate, capacity claim, or prediction about mainnet orders.
- Same-host Docker networking measures a separate consumer process/container,
  not AWS-to-another-region latency. To measure a remote host, run the same
  client where that consumer actually lives with approved DNS/network/identity.

## Safety And Cleanup

The client has a read-only root filesystem, 1 CPU/512 MiB cap, no capabilities,
no Docker socket, bounded tmpfs and PID limit. Only its script, public
  config/contracts and four exact identity files are mounted. Each run records
image digest, tool hash, source SHA, manifest hashes/revisions and timestamps.
The host launcher verifies deletion of its exact `qdl-endpoint-bench-*` container
even on timeout/interruption. A cleanup error is surfaced. It never prunes
images/volumes or touches production containers, Kafka, Redis or SQLite.
SIGKILL/host loss cannot execute a Python `finally`: inspect only that run's
named test container before manually stopping/removing it. No image build cache
is created. Keep compact evidence; remove only the known test output directory
when no longer needed.

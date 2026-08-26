# Phase 10.5 Consumer Cutover And Stable V2 Release

## Purpose

Phase 10.5 promotes only declared market-data requirements. It does not change
an alpha's signal, sizing, execution endpoint, order state, or broker route.
V2 remains provider-neutral behind the shared SDK; V1 stays available only as
the route frozen by the release manifest.

The source contract is
`config/v2/stable-v2-release-routing.yaml`. It is additive to the Phase 10.3
shared-primary route plan: Phase 10.3 seals authority topology, while this file
decides a route for each individual consumer requirement.

## 10.5-A Source Contract

The release manifest binds all of the following by checksum and revision:

- V2 source catalog and canonical identity;
- crypto demand revision;
- capability matrix;
- every registered stable consumer manifest; and
- exact V1 fallback source tag/commit/contract reference.

Each `requirement_key` is the existing stable identity:

```text
instrument_uid:FEED:interval-or-empty:source_policy_id
```

The manifest must enumerate every product in every included consumer; omission,
duplication, a path escape, a changed checksum, a changed manifest revision, or
an unservable V2 product fails before any runtime packet is prepared.
Every materialized V2 product must also remain in the checksum-bound
`stable-crypto-demand` universe. Provider-history pass-through products are
the only deliberate exemption: they use the approved wrapper contract, not a
durable demand binding.

Route meanings are deliberately narrow:

| Route | Meaning |
|---|---|
| `V2_PRIMARY` + `V1` | V2 is normal; only a proven semantically compatible V1 product may be used after a V2 operational failure. |
| `V2_PRIMARY` + `BLOCKED` | V2 is normal; no V1 semantic equivalent has been certified, so the affected read must block rather than silently degrade. |
| `V1_PRIMARY` + `NONE` | Explicitly excluded from this V2 release. VN/DNSE remains on V1 until its real-provider gate is accepted. |

`data-layer:v0.1.0` is only the declared V1 image reference. Before a runtime
handoff, the 10.5-B/C packet must verify the running V1 image/source label or
immutable digest against the frozen `v1.2.2` commit; a mutable tag alone is not
release evidence.

## Readiness And Observability Contract

Every consumer/product observation records only bounded operational metadata:

```text
consumer_id, requirement_key, route, reason,
v2_source_age_ms, v2_receive_age_ms, v2_gap_open,
v1_source_age_ms, v1_receive_age_ms,
consumer_lag, cpu_millicores, rss_bytes
```

No provider payload, credentials, full symbol universe, or unbounded error
body is part of the record. The pure evaluator produces:

- `READY`: all V2-approved products remain V2-primary and explicitly excluded
  products remain V1-primary;
- `DEGRADED`: at least one allowed V1 fallback is active; it is never shown as
  top-level green; and
- `NOT_READY`: an approved product is blocked or evidence is missing/duplicated.

It also fails readiness when a product reports more than `10,000` consumer-lag
records, `750` CPU millicores, or `768 MiB` RSS. These limits come from the
existing Phase 8 lag alert and stable Compose role ceilings; 10.5-B records
actual per-role measurements rather than treating these source defaults as a
capacity certification.

This contract is frozen in 10.5-A. Wiring it into the deployed health endpoint
is a 10.5-B/C task, so a source-only pass does not claim runtime health.

## Ordered Runtime Gates

### 10.5-B: Isolated No-Order Consumer Acceptance

Requires a separate approval that names the isolated V2 ports, private bundle,
consumer identities, paper-only scopes, observation duration and exact cleanup
namespace. Run in order: monitoring, Trading System read-only adapter,
Binance-paper SDK, OKX-paper SDK. For every product, prove warmup, signed
cursor/replay, reconnect, V2 primary, forced permitted V1 fallback, and return
to V2. A `BLOCKED` product must remain blocked. Do not submit an order.

DNSE/VN is not a candidate for this gate until an in-session real-provider
certificate is available.

#### 10.5-B Source Prerequisites And Runtime Packet Template

The source gate binds every external paper workload to its own mTLS identity
and JWT signing key. A known key may sign only its registered manifest subject;
using a Trading System or Binance-alpha key for Monitoring or Alpha-OKX is a
hard authentication failure.

`scripts/phase80_generate_tls.sh` remains the full-CA generator for a fresh,
isolated candidate only. It must not be used to admit these identities into the
running V2 mesh. The narrow path is
`scripts/phase105_prepare_external_consumer_extension.sh OUTPUT_DIR SERVER_CA_FILE`:
it receives only the public CA currently trusted by query/stream, creates a
separate external client CA, two client/JWT identities, and a two-certificate
`client-ca-bundle.crt`. It deletes the external CA private key before returning.
The new runtime option `QDL_STABLE_TLS_CLIENT_CA_FILE` applies that bundle only
when query/stream authenticate clients; their own server certificate and
client-side server trust remain on the existing `QDL_STABLE_TLS_CA_FILE`.

| Consumer | Subject | Client identity | JWT key id |
|---|---|---|---|
| `monitoring.multivenue.stable` | `spiffe://qdl/paper/monitoring-multivenue-stable` | `stable-monitoring` | `stable-monitoring-rs256-v1` |
| `trading-system.paper.stable` | `spiffe://qdl/paper/trading-system-stable` | `stable-trading-system` | `stable-trading-system-rs256-v1` |
| `alpha.binance.paper.stable` | `spiffe://qdl/paper/alpha-binance-stable` | `stable-alpha-binance` | `stable-alpha-binance-rs256-v1` |
| `alpha.okx.paper.stable` | `spiffe://qdl/paper/alpha-okx-stable` | `stable-alpha-okx` | `stable-alpha-okx-rs256-v1` |

The later runtime approval must name all of the following, exactly:

1. The immutable V2 image digest, extension-bundle digest and release-routing
   manifest revision/digest. It must include the
   `QDL_DATA_JWT_KEY_SUBJECTS_JSON` key-to-subject map as well as its public
   keyring.
2. The only V2 endpoints the disposable client may use: query HTTPS
   `127.0.0.1:18201`/`18202`, stream gRPC `127.0.0.1:18220`/`18221`, and the
   private `executor_network`. The probe does not receive a Docker socket,
   provider credential, order endpoint or Trading System execution credential.
3. A one-shot, named helper may copy only `client-ca-bundle.crt` into the
   existing `stable_tls` volume at the query and stream client-trust paths. It
   must not replace `ca.crt`, server certificates, client keys, Kafka stores or
   run `stable_tls_init`. The exact V2 services that then reload the updated
   trust/keyring are `query_v2_1`, `query_v2_2`, `stream_v2_active`,
   `stream_v2_passive`. Kafka, Redis, SQLite, ingestors, projectors, V1,
   Trading System and alpha containers are excluded. Rollback removes only the
   two additive bundle files, restores the prior query/stream environment, and
   recreates only those four named services.
4. A separate, measured packet for `rust_core` if its capacity repair is still
   needed. It must state the old/new memory limit, immutable image, observed
   RSS ceiling and rollback. Do not hide this repair inside the client probe.
5. One disposable client named `qdl-phase105b-acceptance-<UTC>` on a tmpfs
   cursor/state directory, with `--rm --read-only`, bounded to 300 seconds.
   Its only retained output is the payload-free receipt namespace
   `/home/bobby/.local/state/qdl-v2/phase105b-<UTC>/`, removed on a failed
   probe unless an operator explicitly preserves it for diagnosis.
6. A V1 digest-to-commit attestation for the frozen `v1.2.2` fallback before
   any forced-fallback assertion, plus a payload-free binding of that receipt
   to the **currently serving** `data_layer_service` immutable image digest.
   An historical attestation for a different image is not reusable. Without
   both, that assertion is `BLOCKED`, not an equivalent rollback result.

The probe order is Monitoring, Trading System read-only adapter, Binance paper
SDK, then OKX paper SDK. Each approved V2 product must pass warmup, signed
cursor/replay, reconnect and V2-primary receipt checks. Only a product whose
release route explicitly permits `V1` may run the read-only forced-fallback
comparison and return-to-V2 check. `BLOCKED` products must stay blocked;
VN/DNSE receives no request in this gate. Zero order action, direct provider
connection, route mutation, offset reset, Redis/SQLite flush or database write
is an invariant, not a best effort.

The C2 harness is deliberately not a deployed route controller. It runs the
shared V2 SDK receipt first, selects only the frozen local V1 cached endpoint
`http://data_layer:8100` for a `fallback: V1` product, validates symbol,
market, decimal values, final-BAR state and freshness without persisting the
payload, then makes the same V2 SDK read again. The current frozen scope has
`28` V2 products: `10` permitted Binance USD-M V1 fallback drills and `18`
`BLOCKED` routes. A blocked route must produce no V1 HTTP request. This makes
the `V2 -> V1 -> V2` receipt real while leaving every Trading System and alpha
route unchanged.

### 10.5-C: Rolling Consumer Handoff

Requires another approval naming immutable image digests, services, ports,
manifest digest/revision, selected consumer group, V1 rollback revision,
observation window and stop-only rollback. Recreate only named consumer
services. A failure switches only the affected consumer back to V1 and retains
Kafka/cursor evidence.

### 10.5-D: Stable Release Certification

After all approved consumers pass, rehearse a release from immutable images,
verify V1 public compatibility and all V2 manifest/SDK/Rust evidence, record
resource/freshness/gap/lag/fallback metrics, remove only named disposable
artifacts, and publish `2.0.0`. DNSE must either have its separate certificate
or remain explicitly excluded in the capability matrix and release notes.

The review-only source gate is `scripts/phase105_certify_stable_release.py`.
It accepts only explicitly named JSON evidence: exact full-manifest route
observations, frozen V1 provenance, a passed `RUST_PRIMARY` handoff receipt,
the four approved paper-consumer receipts, and an observed V2 -> V1 -> V2
fallback drill for every product whose manifest permits V1. It rejects a
missing or duplicate product, fallback/stale/gapped/over-budget route,
manifest/image mismatch, synthetic runtime evidence, incomplete consumer
class, or secret-like evidence field. Its result records hashes and bounded
metrics only; it has no apply mode and cannot publish or change a route.

Do not run it as an acceptance substitute: Phase 10.5-C must first heal every
demanded final-BAR gap and produce real no-order handoff evidence. A `PASS`
from the gate is necessary, not sufficient, for the separately approved
immutable release rehearsal and `2.0.0` publication.

## Rollback

Use the named V1 route revision from the approved 10.5-C packet. Preserve V2
Kafka/cursor evidence and stop only the V2 consumer workers in that packet. Do
not reset offsets, flush Redis/SQLite, delete data, restart V1, or modify alpha
or order state as part of rollback.

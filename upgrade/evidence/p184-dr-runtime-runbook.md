# P18.4 DR Runtime Runner Handoff

Status: DEVELOPMENT_RUNNERS / RELEASE_HOLD. Follow the current journal for
actual results; this runbook is not a receipt or a shared-runtime packet.

## Provenance

`scripts/p184_dr01_runtime.py` and `scripts/p184_dr08_isolated.py` write JSONL
with the TS `p184.surface-evidence-provenance.v1` header and exact case IDs.
The final TS source SHA/image must come from Claude's final build. Development
uses forty zeroes and `sha256:` plus sixty-four zeroes. Never relabel a rehearsal
or include its failed/pending rows in a final acceptance bundle. Keep its hash
as historical evidence and run again in a fresh output directory.

Before final acceptance, bind actual DL images, TS image, configuration and
test source hashes into the enclosing packet. A caller-supplied header alone
does not attest code running inside an image. No runner grants mainnet/order
authority or permits changing deployed readers/consumers.

## DR01

Use the existing Python image with dependencies, source read-only, a fresh
evidence mount, disposable credentials, bounded CPU/RAM and `--rm`. Initial
DL-only rehearsal uses `--network none`; the HTTP server binds loopback only.

```text
python -B scripts/p184_dr01_runtime.py --development \
  --source-sha <40-zeroes> --image-digest sha256:<64-zeroes> \
  --output /evidence/dr01.rehearsal.jsonl
```

The runner validates the complete source binding artifact and records its hash
and per-venue product counts before deriving disposable identities. The initial
fixture compiles one BAR-1m ALPHA projection per venue and exercises
real HTTP authentication plus metadata access. It is not all-r22 data access,
production mTLS, or a claim that TS consumed the sealed binding. Wrong account
cannot be inferred from wrong consumer/subject. Without an account packet,
those two cells explicitly remain NOT_OBSERVED and the runner exits nonzero.

For account checks, run an isolated final-image TS Gateway and isolated
PostgreSQL/Redis. Reuse TS's existing `test_p18e_current_gateway_http.py` setup
contract; do not modify the shared Gateway or write into shared accounts.
Create two active test alphas with separate generated API keys, each with a
distinct Paper account per venue. Hash keys through TS's existing helper.
Include all four accounts in `WRITER_ACCOUNT_SCOPE_JSON`, enable API-key auth,
disable plaintext migration, and seed distinguishable test-only balances.

Use `--account-packet /private/account-packet.json`; the exact schema is in
`load_account_packet()`. It binds source/image, literal loopback Gateway URL,
two venue pairs, account/alpha IDs, expected decimal balances, and external
mode-0600 key files. Run the driver in the Gateway's isolated network namespace,
never host networking. Keep fixture infrastructure within the same isolated
namespace and no broker credentials. No shared ports/networks/mounts.

Per venue the probe requires A->A and B->B nonempty exact balances, A->B on the
same venue returning `403 ACCOUNT_ALPHA_SCOPE_MISMATCH`, and a separate invalid
key refusal. GET-only, no redirects, no order endpoints. This proves application
account authorization, not broker-side permissions. Auth/rate counters may change
only inside disposable Redis. Inspect financial/command tables before/after and
retire credentials with the fixture. Merely setting `GATEWAY_NO_ORDER_ONLY` is
not a security boundary; isolated topology and the GET-only driver are required.

## DR08

The runner's `--orchestrate <fresh-directory>` mode owns its temporary stack.
It uses immutable existing images, no image build, no published ports and a
broker network namespace without an external interface. Other test roles share
only that namespace. Public core configuration is copied read-only, restricted
to the isolated test binding; production configuration is not rewritten.

```text
python3 scripts/p184_dr08_isolated.py --orchestrate <fresh-directory> \
  --development --ts-source-sha <40-zeroes> \
  --ts-image-digest sha256:<64-zeroes>
```

The synthetic input is explicitly test-only. Test Query read-view support is
needed for the actual GetSnapshot RPC, not a locally invented recovery cursor.
Queue1000 and replay10000 remain fixed. Two distinct consumers must demonstrate
slow-consumer overflow, fast-consumer progress, observable control/status/counters,
and recovery via a newer snapshot after OUT_OF_RANGE. Separate configured limits
from measured high-water values; distinguish gRPC receive from TS Redis ACK.
This experiment does not certify production freshness, independent HA or provider
latency. Development rows cannot qualify the final TS matrix.

Preserve failed attempts. Verify `cleanup.json`, no remaining owned container,
no private keys, unchanged shared image/start/restart inventory and disk pre/post.
Keep active images and bar-edge rollback `f7351c3b`; no broad prune. Final TS
provenance requires a fresh run, not editing the development JSONL.

## Release Boundary

v2.2.3 remains HOLD for final DR01-DR08/broker evidence, independent of building
Paper features. [TRADE attribution and proposals](p184-dr01-dr08-trade-attribution.md)
do not authorize LATEST_STATE, a batch-wait change, rollout, push or merge.

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

## DR01: Regression Versus Deployed Runtime

`scripts/p184_dr01_runtime.py` is CONTROLLED_SIM only, including its old account
fixture helpers. It must NEVER close a DR01 runtime cell. Account acceptance
belongs to Claude's TS Gateway runner. New DL script:
`scripts/p184_dr01_deployed.py`; no account cells and no automatic orchestration.

Offline prepare uses the existing alpha deployment compiler with
stable-v2-release-routing.yaml, catalog, policy, reference manifest and original
alpha inventory. It seals full expected binding bytes and source hashes, exact
manifest request policy and a valid catalog product outside access entitlements.
Do not substitute the TS-only stable renderer for the alpha compiler.

```text
python3 -B scripts/p184_dr01_deployed.py prepare \
  --root /home/bobby/.worktrees/data-layer-p184-closure \
  --inventory <original-alpha-inventory.json> \
  --deployment <deployment-id> --venue BINANCE --output <fresh-binance-packet.json>
```

Repeat for OKX. Host prepare is offline; it does not mount credentials or call
Query. If the exact deployed catalog is supplied with --catalog, its hash is
recorded too. Prepared packets are secret-free but must be retained read-only.

Claude runs probe through stdin with the existing alpha-noorder-sdk2b.compose.json
identity/mTLS mounts, once per venue, sequentially. Use a disposable compose run,
not exec into a running strategy. Preserve the deployed image/identity; do not
start dependencies, change credentials/entitlements, or launch the alpha entrypoint:

```text
docker compose -f <alpha-noorder-sdk2b.compose.json> run --rm --no-deps -T \
  --entrypoint python -v <packet.json>:/run/p184/dr01-packet.json:ro \
  <alpha-service> -B - probe --packet /run/p184/dr01-packet.json \
  < scripts/p184_dr01_deployed.py > <fresh-venue-rows.jsonl>
```

Only four sequential GETs per venue: positive QUOTE from binding, same signed
identity with environment=live (401 UNAUTHENTICATED), revision13 (401
UNAUTHENTICATED), and real out-of-entitlement instrument/feed (403
PERMISSION_DENIED). Normal JWT14, realm paper, TLS CA/hostname/client certificate
checks remain enabled. No retries/proxy/redirects. Positive failure, drift or
transport failure cannot become negative PASS. Token/signing material is never
printed or copied. Budget:4 requests per venue, no streams or provider-direct.
Save compose image/config hashes with the packet; caller-supplied TS header alone
is not image attestation. `probe` imports SDK credentials, not DL server code.

BINDING_COMPILE compares canonical compiled bytes/hash with the actual
/run/p184/binding.json (or declared route-manifest env path). Source inspection
and offline compilation on2026-10-08 found alpha bindings release_revision27,
JWT14, not release_revision22. Both BB deployment hashes matched mounted files:
Binance bbc8b0de1194778f70f94d5cd003fa251f3084250dee3590d4944b4e5f8b8fb8;
OKX29e321e0c6067b34070227598fe8d694f3b491d017ffaa25d4e1073706225ac4.
This is offline binding parity, NOT a runtime DR01 PASS or permission to rewrite
revision. Keep TS bindingr22 terminology separate from alpha release revision.

Evidence of no wildcard includes parsed concrete selector audit and file hashes.
Archive a bounded human-readable grep alongside the packet:
`rg -n 'instrument_uid:|feed:|source_policy_id:' consumers/stable/alpha-*-paper.yaml`.
Source licensing grants are concrete source IDs in stable_catalog.entitlements;
wire403 is mandatory, not replaceable by this config inspection.

After both probes, and ONLY when Claude confirms CI green:

```text
python3 -B scripts/p184_dr01_deployed.py record --rows <binance-rows> <okx-rows> \
  --ci-confirmed --source-sha <CI-green-TS-SHA> --image-digest <TS-digest> \
  --output <new-dr01-dl.jsonl>
```

Recorder requires both venue rosters, merges NO_GLOBAL_GRANT into one all-venue
cell and emits exactly7 DL cells. Claude separately supplies2 WRONG_ACCOUNT_DENIED
cells; never fabricate them here. Output is exclusive, no receipt overwrite.
Provisional TS source2abbe394ea0fb8f7f5ebcbb745ff70a8c83c9769 and
sha256:a6548ee36aeb0242c1734c071678c525c1506d05c10a70b533eb7110e7eaf16a
are NOT certified by this handoff; CI confirmation still required.

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
Query provenance MUST explicitly name qdl-v2-python:2.2.3-rc2-43bbcf0 and actual
inspected digest77682eea; this is NOT production Queryv2.2.2 evidence. Final mode
requires --restart-samples5 (default5; allowed5-10), each a new consumer process
with an independently observed advancing trade. Receipt retains raw monotonic
start/receive times, sample list, n, p50 and max in ms. The first OUT_OF_RANGE to
snapshot recovery remains n=1, reported separately from repeated restart latency.
Old developmentn=1 is not promoted. No final measurement has run in this slice.
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

### Continuity Regression Addendum (2026-10-08)

Exact replay10000 now requires the actual LIVE control with count10000 and a
subsequent newly published live marker. Receiving10000 then cancelling is not
successful replay-to-live evidence. TRADE wire runner is not BOOK qualification.
Prolonged TRADE/BOOK_DELTA handoff stalls are also covered by native direct-service
regressions: terminal error survives a full data channel, fast peer advances,
ordered suffix replay succeeds and task resources return to baseline.

For a changed Rust executable only, development orchestration accepts
`--stream-binary <absolute candidate path>`; it copies a bounded executable into
the owned work directory, records SHA256/bytes separately from the base image,
verifies it inside the driver, and removes the copy during cleanup. This option
is forbidden in final qualification. It does not claim an image rebuild or
allow shared rollout. Keep the original binary and hashes for reviewer replay.
See p184-dr08-continuity-closure.md for counterfactual, wire and trace limits.

## Release Boundary

v2.2.3 remains HOLD for final DR01-DR08/broker evidence, independent of building
Paper features. [TRADE attribution and proposals](p184-dr01-dr08-trade-attribution.md)
do not authorize LATEST_STATE, a batch-wait change, rollout, push or merge.

# Quant Data Layer v2.3.2

Status: **prepared offline, not released, not pushed.**
- Release branch: `release/v2.3.2-session-alive`, in worktree `/home/bobby/.worktrees/dl-v232-release`.
  - It starts from `origin/dev` `7a9c00b` (the v2.3.1 release record).
- Certificate: `CERTIFICATE_DRAFT.md`, a draft that is not issued.
- Rollout: `ROLLOUT_PLAN.md`. It covers Stream x2, both ingestors and the realm bundle. It requires the TS v2.1.5
  writer to be live first.

v2.3.2 is the Data Layer side of the quiet-QUOTE class fix decided by the owner on 2026-11-17 and 2026-11-18:
- Risk accepts an unchanged (quiet) OKX QUOTE only while the writer's per-record session proof is at most 2 s old.
  That rule is unchanged.
- v2.3.2 lets the Stream prove the provider session directly, so the proof no longer depends on 666 ms Query probes.
- The Stream also carries the binding's on-change verdict, so the writer never has to synthesize it.

## Contents (since v2.3.1)

1. **Stream SessionAlive frames.**
   - Merged from `feat/dl-v232-session-alive-frame`: `cdeb211`, then `414324b` and `2358766`.
   - Contract (additive):
     - `SubscribeRequest.session_alive_interval_ms = 9`: 0 means off; the server clamps the value to 250..500.
     - `StreamRecord.payload.session_alive = 12`: a `SessionAlive` message with fields 1..7, schema name
       `qdl.stream.session-alive.v1`.
   - The gateway reads the ingestors' `qdl.provider-session-liveness.v1` records (`SessionLivenessReader`, a port of
     `qdl/runtime/session_liveness.py`). It emits a frame only when all of these hold:
     - the subscription is LIVE and its queue is empty;
     - this replica has caught up with the broker head;
     - the session record is LIVE, has the same session id, generation and config revision as the instrument's last
       event, and is no older than min(3 × interval, SLA).
   - Otherwise the gateway stays silent, so the proof ages out (fail closed).
   - A request without a liveness dir is refused INVALID_ARGUMENT `SESSION_ALIVE_UNAVAILABLE`. A wrong route is
     refused INVALID_ARGUMENT `SESSION_ALIVE_INVALID`.
2. **Opt-in DELIVERY_ON_CHANGE on Stream QUOTE records.**
   - Contract:
     - `QualityFlag.QUALITY_FLAG_DELIVERY_ON_CHANGE = 18`;
     - `SubscribeRequest.delivery_on_change_flag = 10`.
   - The flag is opt-in because pre-2.3.2 readers refuse an unknown QualityFlag value: `qdl_sdk/projection.py`
     calls `QualityFlag.Name`, and the TS writer returns `KernelError` from `view.rs`.
   - The rule matches Query (`stable_source.py:1160-1167`): binding `delivery_semantics == ON_CHANGE`, QUOTE only.
   - The value travels through the gateway bundle. `kn_gateway_bundle.py` emits `delivery_semantics` only for
     ON_CHANGE bindings. A missing field means false, which is fail closed.
3. **Ingestor session-liveness cadence 1000 → 250 ms.**
   - Declared in `stable_deployment.py` as `STABLE_SESSION_LIVENESS_WRITE_INTERVAL_MS`, and carried by
     `refresh_v2_native_ingestor_runtime.py` and `dl_declaration.py`.
4. **Compose.**
   - `stream_kn_1/2` get a read-only `stable_state` subpath `runtime/session-liveness` at `/session-liveness`, and
     the env `QDL_KN_SESSION_LIVENESS_DIR`.
5. **SDK 2.7.0.**
   - `GrpcStreamTransport(session_alive_interval_ms=0, delivery_on_change_flag=False)` and `SessionAliveEvent`.
   - Old behaviour is unchanged unless the caller opts in.
6. **TEST gateway example and isolated e2e.**
   - `examples/v232_quiet_quote_e2e.rs`, run against the TS v2.1.5 writer, passed all three checks:
     - the flag reaches the opted-in QUOTE;
     - SessionAlive keeps a quiet QUOTE admissible: 30 of 30 samples, max proof age 741 ms, 0 probes;
     - a 90 s Stream outage does not exhaust the worker: it recovered 6 s after the restart.
   - Evidence: `~/.local/state/qdl-v2/v232-quiet-quote/e2e-20261118/`.
7. **Benchmark fix (merged from `fix/dl-v232-benchmark-snapshot-batch` `2181f36`).**
   - `snapshot_batch` now measures warmup-free QUOTE probes only, and client-image import gaps count as client
     errors.
   - Includes the quiet-QUOTE RCA note `upgrade/DL_V232_QUIET_QUOTE_SESSION_PROOF_DESIGN.md`.

## Compatibility

- Additive only. buf breaking passes against phase1, phase7-beta and the v2.3.1 base (preflight below).
- A pre-2.3.2 client never sets field 9 or field 10, so it gets byte-identical records and no SessionAlive member.
- An old Stream or projector reading a bundle with `delivery_semantics` ignores the field. The parser in `origin/dev`
  `gateway_bundle.rs` reads named fields only, and the bundle hash is canonical over the whole document.
- The TS writer v2.1.5 always sets field 10. It sets field 9 only when `MARKET_DATA_NATIVE_SESSION_ALIVE_MS` is set,
  and that stays off until this release is live.

## Contract line

`scripts/schema_line.py check` PASSES on contract line 1. A first run reported a false "breaking" result: buf could
not create its cache dir in the container. The rerun used `BUF_CACHE_DIR=/tmp/buf` and passed (preflight below). The
`schema_digest` stays on line 1.

## Not in this release

- The R0 images are built (`R0_SECURITY.md`): rust `646378c6…` and python `7068ad11…`. cargo-deny, trivy and
  pip-audit are still OPEN.
- The realm successor is gated against the deployed stage C r8 realm (rollout P2): 102 ON_CHANGE QUOTE marks, and
  nothing else changed.

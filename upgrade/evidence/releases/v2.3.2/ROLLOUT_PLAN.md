# DL v2.3.2 rollout plan: resume stage C r8 (S6-S11) on the v2.3.2 images

**Status: prepared offline, not executed.**
- The coordinator runs this after the owner approves exactly this blast radius and rollback (CLAUDE.md section 3).
- Any gate FAIL rolls back that step without asking again.

**Rebased 2026-11-18 onto the stage C r8 state.**
- The stage C window `stage-c-window-20261118T0501Z/RECEIPT-PARTIAL.md` is PAUSED after S5:
  - rust_core ×3 and market_projector ×2 already run r8 (kn bundle `5c272b5c`);
  - S6 `stream_kn_1` was auto-rolled back on the writer gate.
- This plan finishes stage C S6-S11 and carries the v2.3.2 changes in the same recreates.
- Nothing here re-renders from stage-B r7 inputs. That would revert the r8 realm and core state.
- Section 8 of the v2.3.1 rollout plan (the advisory follow-up for the Rust roles and bar_edge) is closed by the R0
  images in section 3. Cores and projectors move to them in section 6, a separate step.

**Rules:**
- Never run `compose up` on kafka*, stable_redis or market_cache.
- Never run `scripts/phaseb_prepare_stable_candidate.py`.
- Recreate one role at a time, using the stage C runsheet helpers (DL `docs/dl-v231-stage-c-runsheet` `bbbc4446`) for
  the stage C gates.

## 0. Precondition: the TS writer with backoff goes live FIRST (incident 2026-11-18 05:36Z)

**What happened.** Stage C S6 (`stream_kn_1`) made native slot 4 exit DATA_NOT_READY. The old supervisor restarted it
5 times in 22 s, then RESTART_EXHAUSTED left 64 slices down.

**Rule.** No Stream, Query or ingestor recreate below starts before the TS v2.2.0 writer is live. That writer carries:
- TS `16790784`: upstream exits are never charged; backoff up to 30 s with jitter; RETRYING visible;
- TS `ba0d9ed2`: per-request Query replica pools;
- TS `cc320a4a`: DELIVERY_ON_CHANGE taken from the Stream; field 10 always set.

The writer image is built from TS `feat/v220-writer` (see the TS packet `upgrade/V215_RELEASE_AND_WRITER_ROLL_PLAN.md`
§5 W1).

**Gates W0 (read only):**
- W0.1: the writer image id equals the v2.2.0 writer build, recorded in the receipt.
- W0.2: the heartbeat carries `worker_retries`, and `native_probe` is present.
- W0.3: `DATA_LAYER_V2_QUERY_URLS` names both replicas, and both carry traffic.
- W0.4: 246/246 READY, continuity 11/11.
- W0.5: `MARKET_DATA_NATIVE_SESSION_ALIVE_MS` is UNSET until step 7.

## 1. Blast radius

| Step | Role or file | From (live, read 2026-11-18) | To |
|---|---|---|---|
| P2 | realm dir | `v2.3.1-stage-c-realm/` (bundle `15bc88ff…`, = pinned r8 `realm-bundle.json`) | `v2.3.2-realm/` = byte copy plus `bundle.json` successor `76672095…` (only `delivery_semantics` added on 102 QUOTE bindings) |
| S6/S7 → 3a, 3b | `stream_kn_1`, then `stream_kn_2` | rust `58bdd282`, realm stage-b r7 | rust R0 `646378c6…`, realm `v2.3.2-realm/`, plus the RO mount `stable_state:runtime/session-liveness` at `/session-liveness` and `QDL_KN_SESSION_LIVENESS_DIR` |
| S8/S9 → 4a, 4b | `query_kn_1`, `query_kn_2` | python `26c3b051`, r7 config | r8 config per the stage C runsheet; image `26c3b051` (unchanged) or v2.3.2 python `7068ad11…` (owner choice, section 3) |
| S10 → 5a | `ingestor_binance_usdm` | rust `58bdd282`, config rev 19, cadence 1000 ms | same image (or R0), config rev 24 (r8 bar lane 96) plus cadence 250 ms |
| 5b | `ingestor_okx_swap` | rust `58bdd282`, cadence 1000 ms | same image (or R0), cadence 250 ms only |
| S11 → 5c | `binance_bar_edge` | python `a235c0a1`, out6 pair | v2.3.2 python `7068ad11…`, r8 pair (retained 1047, added 273, dropped 0) |

**Not touched:**
- Kafka x3, stable_redis, market_cache;
- rust_core x3 and market_projector x2 (already r8; their image moves only in section 6);
- the kn bundle `5c272b5c`;
- declarations beyond r8; the TS manifest r14 and binding r26; the JWT manifest revision;
- certificates, the cursor key and the secrets;
- Paper, PM and portal roles.

The TS writer is changed only in the TS windows W1 (before) and W3 (step 7).

## 2. Variables (no secrets)

```bash
Q=/home/bobby/.local/state/qdl-v2; R=$Q/releases; P=qdl_v2_stable_candidate
CF=$R/v2.3.1-stage-c-compose/qdl-v2-stable-kn.stage-c.yml        # stage C compose; v2.3.2 adds the stream mount/env (P1)
SC="--env-file $R/v2.3.1-compose/qdl-v2-stable-kn.v2.3.1.env --env-file $R/v2.3.1-stage-c-compose/stage-c.env"   # live r8 list (rust_core labels)
NEW_ENV=$R/v2.3.2-compose/v2.3.2.env      # SC list + QDL_IMAGE_STREAM=<R0>, QDL_CONSUMER_REALM_DIR=$R/v2.3.2-realm
TLS=$Q/tls/consumer-ca-20261111T1304Z/deployed-tls.env; GEN=$Q/tls/gen-20261019T084200Z/generation.env
SECRETS=$R/v2.2.5/kn-secrets.env          # never cat
C="docker compose -p $P -f $CF"; WIN=$Q/v232-window-$(date -u +%Y%m%dT%H%MZ); mkdir -p -m 700 $WIN
```

## 3. Preparation (offline, done or to do before the window)

**R0 images (done 2026-11-18; DL `d37e88ca`, receipt `~/.local/state/qdl-v2/build-v2.3.2-2401393/receipt.json`):**
- rust `qdl-v2-rust:2.3.2-2401393` = `sha256:646378c6dcfbbf9b01e1b58ba41dd57ef98a847e3236ad531eafa8afb4fc2901`;
- python `qdl-v2-python:2.3.2-2401393` = `sha256:7068ad1101525d20aa33475af78eeff8eccba0bb404ef64d34b1f0ad50d249a8`;
- binary expectations PASS. See `R0_SECURITY.md`.
- **OPEN gates:** cargo-deny (ignore = []), trivy, pip-audit. These must PASS before the window.

**P1: compose and env.**
- Take the stage C compose `qdl-v2-stable-kn.stage-c.yml` and add the v2.3.2 stream mount and env. These are the
  `deploy/compose/qdl-v2-stable-kn.yml` lines 1104/1138-1143 and 1204/1238-1243 of the release tree.
- Gate: `$C $SC … config` and `$C --env-file $NEW_ENV … config` differ only in the two stream services: image,
  mount, env and realm dir.

**P2: realm bundle successor (gate PASS 2026-11-18).**
- `scripts/v232_realm_bundle_successor.py --base $R/v2.3.1-stage-c-realm/bundle.json --catalog
  $R/stage-c-r8-20261117/pinned/repo/config/v2/stable-source-bindings.yaml`.
- The base is the deployed r8 file. It is byte-equal to the pinned `realm-bundle.json` (`15bc88ff…`).
- Result:
  - 1578 bindings, all equal except for `delivery_semantics: ON_CHANGE` on 102 QUOTE bindings (OKX SWAP 97,
    BINANCE USDM 5);
  - manifests unchanged;
  - new sha256 `76672095536f41fdcb449e2d3cf7b5289cff143de8c5f95b3af66848428d8167`.
- Compatibility: the kn bundle successor gives the same 102 marks (`5a5ab39a…`). It is not deployed. The projector
  keeps `5c272b5c`.
- Window prep: `rsync -a $R/v2.3.1-stage-c-realm/ $R/v2.3.2-realm/` (same owner and modes), then install the
  successor as `bundle.json` (0444). Gate: `diff -r` shows only `bundle.json`.

**P3: ingestor configs.**
- Render the Binance config as the stage C r8 ingestor config (rev 24, `stage-c-r8-20261117/pinned/ingestor`) with
  `session_liveness_write_interval_ms` 250.
- Render the OKX config as the live config with only that field changed.
- Use `scripts/refresh_v2_native_ingestor_runtime.py` (v2.3.2) for both.
- Gate: the diff from the r8 or live config is exactly that one field.

## 4. Window (stage C S6-S11 order, one role at a time)

**Step 1: baseline (read only, 10 min).** Record:
- the writer heartbeat (246/246, `worker_retries`, `native_probe` QUOTE probes/min);
- DL CPU;
- the session file mtime cadence (about 1 s);
- the stage C T-30 checks: pins, install verify, render 11/11.

**Steps 3a/3b: `stream_kn_1`, then `stream_kn_2` (stage C S6/S7).**
- `$C --env-file $NEW_ENV --env-file $TLS --env-file $SECRETS up -d --no-deps --force-recreate stream_kn_1`.
- Gates:
  - the stage C S6 role gates (bundle hash = `76672095…`, decode 0, subscribers back);
  - `/session-liveness` is mounted read-only;
  - the writer reaches 246/246 within 120 s, and `worker_retries` drains with no RESTART_EXHAUSTED. This is the gate
    that failed at 05:36Z;
  - Redis GET (read only) of an ON_CHANGE execution QUOTE shows `DELIVERY_ON_CHANGE` on a Stream-written row.
- Rollback: `QDL_IMAGE_STREAM=qdl-v2-rust@sha256:58bdd282…` and the stage-b realm dir, as in the S6 rollback.

**Steps 4a/4b: `query_kn_1`, then `query_kn_2` (S8/S9).**
- Stage C runsheet commands and gates: alpha.okx r17 loaded; an r16 token is still accepted (floor 15); TS 246/246;
  MARK/QUOTE p99.
- Rollback: the r7 config with the same image.

**Step 5a: `ingestor_binance_usdm` (S10), with r8 rev 24 and 250 ms.**
- Stage C gates:
  - bindings +91, lane_plan PINNED;
  - 96 members, 0 rejected;
  - Binance QUOTE/TRADE/MARK flowing within 2 min.
- Also: the session file is rewritten every ~250 ms, and the writer has no RESTART_EXHAUSTED.

**Step 5b: `ingestor_okx_swap`, 250 ms only.**
- Gates: as 5a for the cadence; OKX 246/246 back within 120 s.
- Rollback for 5a and 5b: the previous config dirs.

**Step 5c: `binance_bar_edge` (S11), r8 pair on python `7068ad11`.**
- Stage C D6b gates: 0 outstanding derived gaps at +15 min, healed == recorded at +6 h, RestartCount 0.

**Step 7 (separate TS window W3): `MARKET_DATA_NATIVE_SESSION_ALIVE_MS=500`.**
- Gates, over 30 min across the 96 crossec OKX QUOTE keys:
  - proof age over 2 s in 0 samples while the session is LIVE;
  - QUOTE Query probes per minute drop;
  - no `EXECUTION_CONTEXT_STALE` on QUOTE.

## 5. Rollback summary

Each step rolls back alone, in reverse order:

| Step | Rollback |
|---|---|
| 3 | rust `58bdd282` plus the stage-b realm |
| 4 | the r7 config |
| 5a | config rev 19 at 1000 ms |
| 5b | config at 1000 ms |
| 5c | `a235c0a1` plus the out6 pair |
| 7 | unset `MARKET_DATA_NATIVE_SESSION_ALIVE_MS` |

## 6. Follow-up (closes v2.3.1 §8): cores and projectors onto R0 rust `646378c6`

- Roll one at a time: `rust_core`, `rust_core_2`, `rust_core_3`, then `market_projector_1/2`.
- Same r8 configs. No `stable_redis` recreate.
- Gates: v2.3.0 window A D1-D6. Rollback: `58bdd282` per role.
- Run it only after cargo-deny, trivy and pip-audit PASS on R0.

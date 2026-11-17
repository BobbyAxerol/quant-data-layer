# DL v2.3.2 design: Stream "session alive" frame (quiet QUOTE class fix)

- **Status:** DL side implemented on this branch (section 8), `tested locally` only. Not deployed, not pushed.
- **Owner decision:** option (a), 2026-11-17 about 08:00Z. DL v2.3.2 / TS v2.1.5, together with TS interim (b).
  The Risk 2 s rule stays unchanged.
- **Problem and measurements:** `upgrade/DL_V232_QUIET_QUOTE_SESSION_PROOF_DESIGN.md` on branch
  `fix/dl-v232-benchmark-snapshot-batch`.
  - BAND: proof older than 2 s in 8 of 38 one-second samples, with the BBO quiet for up to 56 s.
  - 5-7 of the 96 crossec symbols showed a proof gap in 48 s.

## 1. Goal

A Subscribe stream for an OBSERVE QUOTE requirement receives, at least every 500 ms, typed evidence that the
provider session carrying that instrument is LIVE. This holds whether or not the BBO changed. The TS writer stamps
`ts_session_heard_at_ms` from that evidence, so Risk's `_quiet_quote_at_use` rule
(`services/risk_engine/execution_market_context.py:333-348`, `now - heard <= 2000`) holds while the venue is
healthy. The 666 ms Query probes become unnecessary.

## 2. Where the liveness evidence is today

- Each Rust ingestor lane writes `runtime/session-liveness/<venue-market>/<lane>.json`
  (`qdl.provider-session-liveness.v1`) into the `stable_state` volume, by atomic rename:
  - throttled to `session_liveness_write_interval_ms`, which is **1000 ms** today (allowed 250..5000);
  - code: `rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs:862-917`, config validation at `:595`.
- Query reads it today (`qdl/runtime/session_liveness.py:169-260`, `StableSessionLivenessReader.status`). It mounts
  `stable_state` subpath `runtime/session-liveness` read-only at `/session-liveness`
  (`deploy/compose/qdl-v2-stable-kn.yml:883-886`, `:1036-1039`).
- **Stream does not.** `stream_kn_*` has no session-liveness mount, and `rust/qdl-stream-gateway` has no reader for
  it. Grep of `rust/qdl-stream-gateway/src`: no session-liveness code.

## 3. Contract (additive, versioned)

`contracts/proto/qdl/query/v2/query.proto`:

```proto
message SubscribeRequest {
  ...
  // Additive (SDK 2.7.0 / DL v2.3.2). 0 (absent) = no session-alive frames,
  // the pre-2.3.2 behaviour; a server that knows the field clamps it to
  // [250, 500] ms. Valid only for a requirement whose route is event-recency
  // OBSERVE (QUOTE/TRADE); otherwise INVALID_ARGUMENT.
  uint32 session_alive_interval_ms = 9;
}

message SessionAlive {                     // qdl.stream.session-alive.v1
  string source_session_id = 1;            // the provider connection the requirement is routed on
  uint64 connection_generation = 2;
  uint32 config_revision = 3;
  uint64 provider_last_transport_at_ns = 4; // from the ingestor record (provider clock domain = DL host)
  uint64 provider_session_liveness_ms = 5;  // gateway now - last_transport, at emission
  uint64 gateway_epoch = 6;                 // the gateway's ownership epoch (fence)
  uint64 high_watermark = 7;                // the subscription's committed watermark at emission
}

message StreamRecord {
  ...
  oneof payload {
    qdl.marketdata.v2.EventEnvelope event = 10;
    StreamControl control = 11;
    SessionAlive session_alive = 12;       // additive; never sent unless requested (field 9)
  }
}
```

**Signing.** The frame carries `resume_token` = the subscription's current signed resume token, unchanged, so it
cannot move a cursor. Together with `gateway_epoch`, the frame is bound to this subscription and this gateway
generation. A frame never advances `logical_offset`; it is set to 0, as for controls.

**Fail-closed rules:**
- No frame when the session record is missing, unreadable, older than 3 × the interval, `state != LIVE`, of a
  different session or generation than the instrument's last event, or of a different config revision.
- No frame while the subscription is REPLAYING, in BACKPRESSURE or RECOVERY_REQUIRED.
- Silence is the signal: the writer's proof simply ages out, and Risk refuses as today.

**Compatibility:**
- Old clients never set field 9, so they never receive field 12.
- An old Python SDK that did receive it would ignore it: `qdl_sdk/transport.py:640-655` branches only on `control`
  and `event`.
- The Rust TS writer decodes with prost; an unknown oneof member becomes `payload = None` and must be skipped. The
  TS change below adds that guard first.
- Schema line: add `qdl.stream.session-alive.v1` to `contracts/schema-lines`, and regenerate
  `rust/qdl-stream-gateway/src/generated.rs` and the SDK protobuf.

## 4. DL implementation plan (this branch, next slice)

1. **Proto and generated code**, as section 3, plus a buf-breaking check against the base: additive only.
2. **Compose (blast radius):**
   - `stream_kn_1/2` gain the same read-only `stable_state` subpath mount as Query, at `/session-liveness`.
   - Ingestor `session_liveness_write_interval_ms` goes 1000 -> 250 for the OKX and Binance lanes that carry QUOTE.
     With a 500 ms frame cadence, the worst proof age is then about 250 + 500 + transport, well under 2 s; with
     1000 ms it would be about 1.5 s, too tight.
   - This is a declared ingestor config change, so it needs an ingestor recreate.
3. **Rust gateway** (`rust/qdl-stream-gateway`):
   - a `SessionLivenessReader`, a port of `qdl/runtime/session_liveness.py` `status()` with the same validation, file
     regex and caps, cached by `(inode, mtime, size)`;
   - in `subscription.rs`, a `tokio::time::interval(session_alive_interval)` arm in the live loop that looks up the
     instrument's last event's `(source_session_id, connection_generation, config_revision)` (kept per
     subscription), reads the status and emits `SessionAlive` only when LIVE;
   - frames go through the same bounded handoff as events. They are dropped, never queued, when the handoff is full,
     because a stale "alive" frame is worse than none.
4. **SDK 2.7.0:**
   - `SubscribeRequest.session_alive_interval_ms` is exposed on `GrpcStreamTransport` (default 0);
   - a new `SessionAliveEvent` model is yielded only when requested.
5. **Tests:**
   - Rust unit: a LIVE record produces a frame; DISCONNECTED, stale, wrong generation or revision, and replaying
     state produce none; a full handoff drops the frame.
   - Rust: a frame never changes `logical_offset` or `resume_token`.
   - Contract: proto golden and schema line.
   - SDK: an old client ignores field 12; a new client parses `SessionAliveEvent`.
   - Integration, with an in-process gateway and a fake session dir: frames every 500 ms ± 50 ms while LIVE; they
     stop within one interval after DISCONNECTED.

**Cost estimate**, to be measured after implementation:
- Frames: one per OBSERVE subscription per 500 ms. For TS r14 that is the 102 QUOTE plus 11 TRADE routes, so about
  **226 frames/s**.
- Size: at about 120 bytes each, about 27 KB/s.
- CPU: at the gateway's per-record encode/send cost, an assumed 20-50 µs per record (not measured), about
  **0.005-0.011 vCPU** on the two streams together.
- Saving on the Query side: the writer's QUOTE probes stop, about 3,000 Query requests/min (B4 measurement). At the
  Query cost seen in v2.3.1 that is far larger than the frame cost.
- Session file reads: one cached `stat` per lane per interval, negligible.

## 5. TS side (`feat/v215-writer-session-alive`, from TS dev 4967aa9e)

**(b) interim, no DL dependency.**
- In the Rust market-data-writer, stamp `ts_session_heard_at_ms` / `ts_session_budget_ms` on the stream-path QUOTE
  write too, not only on Query reads (`view.rs:926-929`).
- The value is the event's receipt minus the DL quality `provider_session_liveness_ms` carried on the stream view.
- This closes the records seen with no proof (APE, DOGE in the 96-symbol sweep).

**(a) consumer, behind `MARKET_DATA_NATIVE_SESSION_ALIVE_MS` (default unset = off):**
- When the flag is set, send `session_alive_interval_ms` on QUOTE/TRADE OBSERVE subscriptions.
- On each `SessionAlive` frame, re-stamp the proof on the cached QUOTE of that instrument, with
  `heard = receipt - provider_session_liveness_ms`, and skip the 666 ms probe for that route
  (`session.rs:374-389`) while frames keep arriving.
- Fall back to the probe after 2 missed intervals.
- Decode guard: a `payload = None` record (unknown member) is ignored, not treated as a protocol error.
- Tests: the writer's own CI image, run locally.

## 6. Why the implementation is not in this branch yet

This slice changes:
- the stream proto;
- the Rust gateway (a new reader and a new loop arm);
- the stream compose mount;
- the ingestor liveness cadence;
- the SDK;
- the TS Rust writer.

It also needs a gateway integration test in a container. Under the current host lock (until 09:03Z) and the no-test
window (until 09:05Z), none of it can be tested now. A half-implemented proto/gateway change on a shared branch
would block the TS side.

Recommended next slice: a dedicated implementation pass on this branch in the order of section 4, then TS section 5
(b) first, because it has no DL dependency.

## 7. Replay proof plan (offline)

- **Input:**
  - the read-only Redis samples of BAND and GAS (1 s cadence; observed_at, ts_session_heard_at, liveness);
  - the ingestor session-file cadence of 1000 ms or 250 ms.
- **Model:** for each 1 s sample, `stale_before = now - heard_at > 2000` (the measured writer proof).
  `stale_after_a = (now - last_frame_heard) > 2000`, where frames arrive every 500 ms carrying
  `liveness = now_file_age`.
- **Output:** the fraction of seconds stale, before and after. BAND before: 8/38 = 21 %. After: expected 0 % while
  the session is LIVE.
- The simulator belongs with the implementation slice. The measured BAND/GAS samples are kept in
  `~/.local/state/qdl-v2/v232-quiet-quote/` and are not in Git.

### 7a. Replay result (offline model, 2026-11-17; `~/.local/state/qdl-v2/v232-quiet-quote/replay-before-after.txt`)

| Symbol | Before (measured: proof older than 2 s at 1 s samples) | After (model: frames every 500 ms, measured DL liveness) |
|---|---|---|
| BAND-USDT-SWAP | 10/52 = 19 % | 0/2000 = 0.0 % (1000 ms and 250 ms session files) |
| GAS-USDT-SWAP | 3/12 = 25 % | 0/2000 = 0.0 % |

The "after" column is a model: frame lag U(0, 500 ms) plus the measured `provider_session_liveness_ms` (max 1007 ms).
It assumes frames are not delayed. The implementation slice must confirm it on a live replay.

## 8. Implementation (DL v2.3.2 slice 1, 2026-11-17)

What was built, and where it differs from sections 3-4. Each difference is fail-closed.

**Contract** (`contracts/proto/qdl/query/v2/query.proto`): exactly section 3 (field 9, `SessionAlive` 1..7,
oneof member 12).
- `config_revision` is the revision the instrument's last event carries. The session record may be newer, because a
  retained binding keeps the revision it was added at (the Query reader rule since v2.3.0 rc3). A record older than
  the event gives no frame.
- `gateway_epoch` is the gateway process start time (Unix ns). The gateway has no ownership epoch, so this names the
  instance that judged the frame.
- `contracts/schema-lines` registers compatible lines of `market_data.proto` only. It has no per-message entries, and
  an additive change stays in line 1, so nothing is added there. The name `qdl.stream.session-alive.v1` is in the
  proto comment and in `session_liveness.rs` (`SESSION_ALIVE_SCHEMA`).

**Gateway** (`rust/qdl-stream-gateway/src/session_liveness.rs`, `service.rs`, `hub.rs`, `main.rs`):
- `SessionLivenessReader` ports `StableSessionLivenessReader.status`: the same record validation, file rule, 256-file
  cap, flags and `(inode, mtime, size)` cache. Two differences: the lineage successor is not ported, and a record
  file over 4 KiB is refused.
- The Subscribe live loop gets an interval arm, created only when field 9 is set. Each tick emits a frame only if
  all of these hold:
  - auth is valid and the subscription is not terminal;
  - the queue holds nothing for this product;
  - the hub reader is caught up on the partition (`Hub::reader_caught_up`: the hot-read rules, broker head confirmed
    within 1 s);
  - the session matches the last delivered event and is LIVE;
  - the transport age is at most 3 x interval and within the requirement's `max_session_liveness_ms`.
- The frame goes through `try_send`. A full handoff drops it and counts it (`session_alive_dropped`).
- A stream that has delivered no event yet takes the session from the newest committed record of its key in the hot
  ring, and only when that record is at or below the stream position. This covers a quiet instrument right after
  subscribe.
- Field 9 on a route that is not event-recency OBSERVE QUOTE/TRADE: INVALID_ARGUMENT `SESSION_ALIVE_INVALID`.
- `QDL_KN_SESSION_LIVENESS_DIR` unset: INVALID_ARGUMENT `SESSION_ALIVE_UNAVAILABLE`. Set but unreadable: the gateway
  refuses to start.

**SDK 2.7.0:** `GrpcStreamTransport(session_alive_interval_ms=0|250..500)` and `SessionAliveEvent`; the default
sends 2.6.0 bytes.

**Runtime config, written in the repo and not applied:**
- `stream_kn_1/2` get the Query mount and `QDL_KN_SESSION_LIVENESS_DIR=/session-liveness`.
- The compiler declares `STABLE_SESSION_LIVENESS_WRITE_INTERVAL_MS = 250` for both native ingestors.
- `refresh_v2_native_ingestor_runtime` and `dl_declaration` carry it as a compiler-tuned field. Before this change
  they refused any non-binding difference.

**Consumer rule (TS):** re-stamp a cached QUOTE only when the frame's `source_session_id`, `connection_generation`
and `config_revision` equal the cached event's. A `payload = None` record is skipped.

**Measured cost** (`rust/qdl-stream-gateway/tests/session_alive_cost.rs`, in-process, opt-level 3; no HTTP/2 or TLS):
- build and encode: about 0.27 µs per frame (343 bytes);
- the session status read: 34.8 µs with 4 lane files and 98.9 µs with 16, mostly the directory scan;
- at 226 frames/s that is 0.008 to 0.022 vCPU. The section 4 estimate was 0.005 to 0.011.

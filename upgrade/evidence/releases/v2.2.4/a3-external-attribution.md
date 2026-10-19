# Amendment A3: external attribution of G2 tails, G4 samples and G9 eligibility losses

Status: **FROZEN before the candidate run** (frozen file `config/v2/p184-stage100-a3-external-attribution.json`; its
sha256, frozen_at and the old -> new apparatus hashes are recorded in the file itself and in
`DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`). Version 1.

- Decision owner: Bobby (owner). Approval: AskUserQuestion 2026-10-18 ~10:10Z, option "Amendment A3 quy nguyên nhân
  (Recommended)": a frozen, versioned external-attribution amendment and ONE final stage-100.
- Review input (recommendations, not approvals): Codex `p184-idr112-113-provider-gates-review-20261018.md`
  ("Gate Amendment Options") and `p184-release-closure-options-20261018.md` sections 1-4.
- Code: `scripts/p184_a3_attribution.py` (pure classification), `scripts/p184_a3_evidence.py` (host-local sampler,
  post-load lane timeline), `scripts/p184_g4_observer.py` v7 (exceedance retention), `scripts/p184_g4_binance_probe.py`
  (Binance fresh connections), `scripts/p184_gate_campaign.py` (application), `scripts/p184_freeze_a3_amendment.py`.

## Why

The last three final runs failed G2/G4/G9 on samples whose recorded evidence pointed at the venue or one venue session:
Binance session resets 09:32:55-09:33:32Z in 092255Z (G2 TRADE/QUOTE/MARK BINANCE, ten G9 losses), OKX book tails on
lane `okx-public-001` with a healthy fresh connection in the same seconds (074624Z, 092255Z). The frozen gates cannot
tell a venue-originated sample from an internal one. A3 lets a sample leave the verdict only with evidence for that
sample, and keeps every other sample, gate and threshold as frozen. External origin is not a claim that no local
resilience action exists (Codex); A3 changes scoring only.

## Rule (one sentence)

A unit is EXTERNAL only if, for the SAME event (or for a G9 loss: the same seconds on the slice's own lane), (i) a
fresh-connection probe recorded that very venue event (same id AND venue timestamp) on time while the production lane
got it late, or the ingestor logged a VENUE-originated session end on the unit's lane with a complete joined timeline; (ii) the unit's internal
segments and the host-local checks at its exact seconds are inside the frozen limits; and (iii) the host clock bound is
recorded on both sides. Otherwise it is UNKNOWN, and UNKNOWN counts as FAIL exactly as before.

## Per gate

| Gate | Unit | Attributable in v1 | Evidence |
|---|---|---|---|
| G4 | one ADMITTED observer event above its row limit (frozen-table p95 limit, GATED addendum max limit) | yes | observer v7 retains every such event with exact clocks and identity (cap 256 per key/window; overflow -> INCOMPLETE); the window max is always retained |
| G2 | one new-event transit above the row limit | **no** | native writer slices carry no per-sample receive/canonical/dispatch clock; Python MARK records have `envelope_received_at_ns` NOT_MEASURED (Query reference basis). Every G2 tail is UNKNOWN; G2 is scored as before; lane episodes are reported |
| G9 | one data-path eligibility loss (a slice newly in `not_ready`, not TRADE_EVENT_STALE) | yes, by lane episode | lane timeline + host-local + DL-path checks + restoration sample; `demanded == session-ready` and v1 fallback stay strict |

Amendment 1's row keeps its own rule; REPORT_ONLY rows stay report-only; G1, G3, G5-G8, G10, G11 are untouched; G12
and G13 are inherited (PASS / 76/76), not rerun.

## Evidence and limits

- Same-event join: DL's canonical `source_sequence` carries the venue id: OKX books `g<gen>:<seqId>`, Binance depth
  `g<gen>:<u>`, Binance quote = bookTicker `u`, trade = trade id, mark = `E:...` (observed in 092255Z records). The
  OKX books probe records seqId per push; the Binance probe records (channel, symbol, id, E, T, receipt) per event. A
  healthy *different* event in the same second is supporting only (UNKNOWN).
- `EXTERNAL_SESSION_UPSTREAM`: same event on the fresh lane <= 150 ms (the books probe's HEALTHY bound) and the
  production lane >= 50 ms later. `EXTERNAL_VENUE_WIDE` (probe late too) is defined by the owner but **not assigned in
  v1**: the probe runs at 0.5 CPU on the same host and records no scheduler evidence of its own, so its lateness can be
  its own; a late probe is UNKNOWN.
- `EXTERNAL_VENUE_RESET` (G4) / `EXTERNAL_SESSION_INTERRUPTION` (G9): a VENUE-originated session end on the unit's
  own lane: OKX `qdl_native_session_disconnected` with cause EOF / CLOSE_FRAME / READ_ERROR / HEARTBEAT_TIMEOUT /
  NOTICE*, Binance `..._disconnected` whose error names a peer reset/close, or `qdl_native_session_heartbeat_failed`.
  DL-initiated ends (STALENESS_HANDOVER = IDR-112 self-recycle of a late lane, BOOK_RESYNC, WRITE_ERROR), connect
  failures and standby failures never start an episode, and one on the lane inside an episode's span makes it
  incomplete. OKX logs name the lane; Binance logs are joined by the unique lane with the faulting generation
  (ambiguous -> UNKNOWN). Faults <= 60 s apart form one episode with: last good frame (progress `last_frame_age`),
  first frame of a newer generation (progress, searched within the recovery budget), another lane of the same ingestor
  reading (the progress interval after each fault, a missing read_gap fails), and for G9 the restoration sample. G9
  additionally needs a session-type reason (TRADE_SESSION_UNVERIFIED / EXECUTION_CONTEXT_NOT_EXECUTION_ELIGIBLE), the
  loss onset between the lane's last good frame and its first fault, every writer worker LIVE and the local and DL-path
  checks clean from the onset to restoration. Recovery is counted from the outage start (last good frame / onset). The per-event canonical coordinate and consumer ACK are not recorded; they are replaced by the DL-path check
  (core raw_age <= 500 ms, gateway dispatch-lag delta bucket <= 500 ms over the recovery seconds) and the writer's
  restoration. This is stated, not hidden.
- G4 internal segments, same event: Kafka CreateTime - DL receipt and observer receipt - CreateTime, each <= the row's
  baseline maximum x 1.2 (recomputed from the addendum's frozen BASELINE campaign, the addendum method; OKX inverse rows
  use their pooled OKX row; rows without baseline samples have no limit -> UNKNOWN), and the internal total (observer
  receipt - DL receipt) <= the row limit itself.
- Host-local checks at the unit's seconds (A3 sampler, whole load): ingestor cgroup throttled periods 0, worst thread
  run-queue wait <= 50 ms per second, no ingestor socket sample with unread bytes, continuous coverage (gap <= 2 s) and
  no sampler error; a missing field fails. Blind spot: samples ~100 ms apart cannot exclude a shorter stall, nor frames
  already read into user space but processed late (not over-claimed).
- Clock: chrony bound sampled every 30 s; samples within 60 s on both sides, each <= 10 ms.
- Caps: per G4 row over all windows AND per window, EXTERNAL units <= 1 % of the admitted samples (so a p95 failure,
  >= 5 % of a window over the limit, is never rescued); G9, EXTERNAL losses <= 1 % of demanded x readable samples and
  <= 3 episodes per venue; any recovery > 60 s -> FAIL.
- Applicability: only a candidate campaign that recorded this file, with the frozen tool, budget, addendum, amendments
  1/2, runtime images, client profile image, workload (100 sessions) and the frozen A3 code (sha256 recorded at run
  time AND equal at score time); preflight refuses a run whose images or code differ. A campaign run with this tool
  for which A3 does not apply also loses amendments 1/2 (their re-pin goes through A3): it is then scored strictly.
- EXTERNAL units stay in every distribution and in the scorecard (count, segment values, recovery); verdicts are
  recomputed on the remaining units with the unchanged thresholds.

## Independent review before the run (2026-10-18)

A read-only reviewer subagent found four blocking loopholes, all fixed before freezing: (1) a late fresh probe made an
internal tail EXTERNAL_VENUE_WIDE -> the class is not assigned in v1; (2) every OKX disconnect, including DL-initiated
STALENESS_HANDOVER, counted as a venue fault -> venue-originated causes only, cause kept by the collector; (3) a G9 episode
did not have to explain the loss onset and the checks skipped the onset seconds -> onset between last good frame and
fault, checks and recovery from the outage start; (4) the scorer did not verify the A3 code sha256 -> run-time and
score-time code pinned. Non-blocking findings fixed: exact clocks instead of rounded visibility, socket-sampler gaps and
errors, missing fields fail, per-core / per-gateway coverage and zero-dispatch intervals fail, per-window cap, G9 loss
multiset compared with G9, session-type reasons and writer liveness, bounded recovery search, workload/profile/pid-stability
checks, preflight refusal on drift, probe venue timestamp must match.

## Apparatus fixed with A3 (A1 and Codex section 1)

- The A1 funding allowance is campaign input data (`query-funding-allowance.json`, schema
  `qdl.p184.query-funding-allowance.v1`, digest + int type + the deployed Query image ids it was read from), resolved in
  preflight from the deployed Query image; missing or inconsistent -> the campaign is refused, no load. The client
  verifier imports no Query symbol and fails closed while unconfigured.
- Exact-client smoke before any sampler or load: the driver in `target-matrix` mode (profile image, mounts, user,
  bootstrap) reads every operation once per replica, BAR warmups, and one stream subscribe/close per venue; failure ->
  refused. An ImportError/NameError/SyntaxError in a client worker exits it at once and aborts the driver (no 30-minute
  error storm).
- The Codex pause watcher pauses the IDE Codex servers and their whole descendant tree (never kills).

## Stop rules (frozen)

One declared run (a NEW run; the 092255Z receipt stays invalid and preserved). Refused before load on a preflight
problem, an unresolved allowance or a failed client smoke. A foreign job before or early in the load (guard TAINTED)
allows one rerun; any other failure is a result. A3 applies only if the campaign's recorded runtime images equal the
frozen ones.

## Runtime-image pin refresh, 2026-10-18 ~19:15Z (not a criteria change)

Why: A3 hard-codes the runtime image ids it was frozen with, and the campaign preflight refuses any other image set
(`scripts/p184_gate_campaign.py` preflight, "A3: runtime images differ from the frozen ones"). The owner-approved
rustls 0.23.46 roll (2026-10-18 ~15:50Z "Nang rustls ngay"; chain "Cu tiep tuc lam cho xong nhu da duyet" ~18:40Z)
replaced the ingestor and stream-gateway image, so the final stage-100 could not start against the A3 of 12:17:51Z.

What was done: the unchanged freeze tool (`scripts/p184_freeze_a3_amendment.py`, sha256 `71d73b5c...`, the pinned
`freeze_tool`) was run again with the same inputs (`--base-ref 6ff401e`, `--base-profile
.../profile-stage100-rc4-r16.json`) after the roll, to a scratch path, and the result replaced the file. The JSON diff
against the previous file is exactly these paths and nothing else:

| Path | Old (12:17:51Z, sha256 `cc5fc7dc...`) | New (19:15:00Z, sha256 `384e8512...`) |
| --- | --- | --- |
| `frozen_at` / `frozen_at_unix` | 2026-10-18T12:17:51Z | 2026-10-18T19:15:00Z |
| `runtime_images.images.ingestor_okx_swap` | `a9679def...` | `27c79b53...` |
| `runtime_images.images.ingestor_binance_usdm` | `a9679def...` | `27c79b53...` |
| `runtime_images.images.stream_kn_1` | `60f83a51...` | `27c79b53...` |
| `runtime_images.images.stream_kn_2` | `60f83a51...` | `27c79b53...` |

Unchanged (same bytes in the JSON): every rule, class, condition, cap, stop rule, numeric limit (including the nine
per-row G4 internal-segment limits recomputed from the same frozen baseline), the workload, the profile image, the
decision record and every apparatus pin (no apparatus file changed since 12:17:51Z, so no tool hash moved: gate tool
`e179ec78`, attribution `dcf3a17c`, evidence `3fec490b`, observer `1d33ca4a`, driver `293e1b1b`, ...). The other
runtime images stay: query `3e9e4d3a` x2, projectors `60f83a51` x2, cores rc22 `7c848aad` x3, writer `2deb90b0`.

Effect on old receipts: none. A campaign applies only the A3 file it recorded (path + sha256) at run time; 123222Z and
122102Z recorded the 12:17:51Z file inside their run worktree (data-layer-p184-final-run-6d17b56f, kept), and every
older receipt recorded no A3. The text `scope.inherited_not_rerun` (G12 / G13 inherited) is part of the frozen
content and was not edited; for the rolled image set G12 and G13 are rerun (journal).

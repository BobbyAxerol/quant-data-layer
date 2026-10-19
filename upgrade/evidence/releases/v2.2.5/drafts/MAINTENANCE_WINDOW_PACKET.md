# Data Layer v2.2.5 maintenance window packet (DRAFT, not executed)

Status: DRAFT for the owner's go/no-go. Nothing in this packet has been executed on production. The procedure was
rehearsed on an isolated stack (section 7). Target window: before 2026-11-06; hard deadline 2026-11-20 14:43Z (expiry
of every deployed mesh certificate).

**This window is a full-mesh outage, not a config-only change.** The CA changes and every peer authenticates every
other peer, so the TS writer, every Data Layer mesh role and the three brokers stop together; the Trading System
receives no V2 market data from W1 until the writer is ready again after W12 (V1 is a stopped rollback container,
not a fallback). Measured outage in the rehearsal: section 7.

Owner approvals that cover this packet (2026-10-19): one window combining DL v2.2.5 (IDR-127 config, versioned KN
compose, healthchecks) with the atomic mesh mTLS rotation and the TS writer roll to the TS release image; rollback =
old certificates (valid to 2026-11-20 14:43Z) + old digests; K6 (restore the default dirty ratio on
`__consumer_offsets`); keep v2.2.5 compact (owner 09:00Z). The production steps below still need the owner's explicit
go for the chosen date (section 9).

## 1. What changes and what does not

| Area | Before (v2.2.4 runtime) | After (v2.2.5) | Recreated in the window |
|---|---|---|---|
| Mesh TLS | generation 20260822T144323Z (CA `qdl-phase8-certification-ca`, notAfter 2026-11-20 14:43Z) + KN client CA 2026-09-27 (leaves notAfter 2026-12-25) | generation 20261019T084200Z: one CA for the whole mesh, every leaf 365 days (notAfter 2027-10-19), same CN/SAN per identity, `ca.key` deleted | every mesh peer |
| Container definitions | per-role inspect composes under ~/.local/state (pre-OPT caps in some files, drift R1/R4) | `deploy/compose/qdl-v2-stable-kn.yml` (committed), caps = live, parity PASS | all roles below |
| Healthchecks | none on stream_kn x2, market_projector x2, rust_core x3; 2 s / 5 s probes on stable_redis / Query / Kafka | liveness probes added (TCP 8210 / status-file freshness 30 s / TCP 8300); cadence 15 s on Kafka, Query, stable_redis (same tests) | same |
| Query image | qdl-v2-python:2.2.4-31a5155 (3e9e4d3a) | qdl-v2-python:2.2.5-601c3d8 (b7056141; files equal to 3e9e4d3a except the 16 git-expected config/apparatus files, pip freeze equal) | query_kn_1/2 |
| Rust images | 27c79b53 (ingestors, stream), 60f83a51 (projectors), 7c848aad (core) | **unchanged digests** | recreated for mounts/healthchecks only |
| bar edge | qdl-v2-python:2.2.3-rc14-a388537 (08f356a8), held | **unchanged image** (held: a new python image bakes catalog r12 / acquisition r20 and would refuse the r11/r19 checkpoint, section 6) | recreated for mounts only |
| Kafka topic config (K6) | the temporary `min.cleanable.dirty.ratio` 0.05 on `__consumer_offsets` was already removed 2026-10-16 14:12Z (plan journal; observed ABSENT 2026-10-19 10:35Z with `kafka_internal_topic_compaction.py --observe`) | unchanged: K6 is a no-op, W5 only re-observes | no |
| TS writer | local/trading-system:p184-writer-release-30f50a91 (2deb90b0), identity 2026-08-22 | W12-A (default): same image 2deb90b0, identity of the new generation; W12-B only with the TS proof (section 5) | yes |

Not touched: market_cache (projection cache; never recreated for a certificate change), Kafka data volumes, topics,
consumer-group offsets, KRaft metadata (the brokers' dynamic listener truststore path is kept; the new generation
provides that file), JWT signing keys and their public keys, cursor keys, consumer realm manifests (r16 dir), KN
release dir, Postgres, the V1 fallback (`data_layer_service`, stopped rollback container per the coordinator),
every other TS service, portal stacks.

## 2. Pre-window checklist (T-1 day to T-0; no production mutation)

1. `fix/v225-config-certs` merged into dev with green CI; python image built and recorded (section 3).
2. Re-verify the TLS generation on the day:
   `sudo python3 -B scripts/rotate_mesh_tls.py verify --old-env ~/.local/state/qdl-v2/tls/gen-20260822T144323Z/generation.env --new-env ~/.local/state/qdl-v2/tls/gen-20261019T084200Z/generation.env --out <window dir>/tls-verify.json` -> PASS.
3. Re-run compose parity against live (v2.2.4 env + deployed generation):
   `python3 -B scripts/kn_versioned_compose.py inventory --out <window dir>/pre-inventory.json` and
   `python3 -B scripts/kn_versioned_compose.py parity --compose deploy/compose/qdl-v2-stable-kn.yml --env-file deploy/compose/qdl-v2-stable-kn.v2.2.4.env --env-file ~/.local/state/qdl-v2/tls/gen-20260822T144323Z/generation.env --env-file ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env` -> PASS.
   Any problem = a role changed since 2026-10-19: stop and re-render; never apply over drift.
4. Save rollback composes for every role from docker inspect (`kn_production_packet.service_from_inspect`, existing
   practice) into `<window dir>/rollback/`, plus the TS writer's own create compose.
5. TS release image ready with its closure-equivalence proof (section 5); TS owner go.
6. Baseline: writer heartbeat demanded/ready (64/64), Query ready products, consumer-group lag, URP 0, no
   restarts; `docker ps` shows no foreign test containers.
7. Host measurement lock taken by the window owner (3 h); portal stacks paused as for the certified run (IDR-128).

## 3. Images

| Role | v2.2.4 (rollback) | v2.2.5 (target) |
|---|---|---|
| kafka1-3 | apache/kafka@sha256:9516fb76... | same |
| rust_core x3 | qdl-v2-rust@sha256:7c848aad... | same |
| ingestors, stream_kn x2 | qdl-v2-rust@sha256:27c79b53... | same |
| market_projector x2 | qdl-v2-rust@sha256:60f83a51... | same |
| query_kn x2 | qdl-v2-python@sha256:3e9e4d3a... | qdl-v2-python@sha256:b7056141... (2.2.5-601c3d8) |
| binance_bar_edge | qdl-v2-python@sha256:08f356a8... | same (held) |
| market_cache, stable_redis | redis@sha256:dfa18828... | same |

The env files `deploy/compose/qdl-v2-stable-kn.v2.2.4.env` and `.v2.2.5.env` pin every digest.

## 4. Window procedure

Common command prefix (`$C`):

```
C="docker compose -p qdl_v2_stable_candidate -f deploy/compose/qdl-v2-stable-kn.yml \
   --env-file deploy/compose/qdl-v2-stable-kn.v2.2.5.env \
   --env-file ~/.local/state/qdl-v2/tls/gen-20261019T084200Z/generation.env \
   --env-file ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env"
```

Never `$C up` without `--no-deps <services>`; never `--remove-orphans`; never name market_cache.

| Step | Action | Pass condition before the next step |
|---|---|---|
| W1 | `docker stop -t 30 market_data_service_p184w` | exit code 0 (clean stop, cursors committed) |
| W2 | `docker stop -t 45` stream_kn_1/2, query_kn_1/2, binance_bar_edge, market_projector_1/2, ingestor_okx_swap, ingestor_binance_usdm, rust_core, rust_core_2, rust_core_3 (one command) | all exited |
| W3 | `docker stop -t 10 ...stable_redis-1`; `docker stop -t 60` kafka1-3 (one command) | all exited; from here the mesh is down |
| W4 | `$C up -d --no-deps --force-recreate kafka1 kafka2 kafka3 stable_redis` | admin with the NEW generation lists brokers 1,2,3; quorum leader; URP 0 after ISR catch-up |
| W5 | K6 re-observe (no alter expected): `python3 -B scripts/kafka_internal_topic_compaction.py --observe --cert-dir ~/.local/state/qdl-v2/tls/gen-20261019T084200Z/kafka`; record `temp_state` and the decision-C keys. Only if the temporary key reappeared: `--remove-temp --apply --cert-dir <same> --lock-owner <window owner>`; exact restore of the measured pre-window state = `--apply-temp --apply` (only if the pre-window observation showed it present) | temp_state ABSENT, c_state APPLIED, no drift |
| W6 | `$C up -d --no-deps --force-recreate rust_core rust_core_2 rust_core_3` (simultaneous: same image, no mixed-version group) | 3 members in `qdl-v2-realtime-core-v2`, healthy, no retry loop |
| W7 | `$C up -d --no-deps --force-recreate ingestor_binance_usdm ingestor_okx_swap` | healthy (session-liveness fresh) |
| W8 | `$C up -d --no-deps --force-recreate market_projector_1 market_projector_2` | healthy (status fresh), lag falling |
| W9 | `$C up -d --no-deps --force-recreate query_kn_1 query_kn_2` | healthy; `/health/ready` READY over mTLS with the NEW TS identity; ready products back to the baseline |
| W10 | `$C up -d --no-deps --force-recreate stream_kn_1 stream_kn_2` | healthy; TLS handshake with the NEW TS identity verifies |
| W11 | `$C up -d --no-deps --force-recreate binance_bar_edge` (image unchanged) | healthy; log shows checkpoint restore, no bootstrap |
| W12 | TS writer, W12-A: recreate image 2deb90b0 from its saved create compose with only `/run/secrets/qdl-v2` -> `~/.local/state/qdl-v2/tls/gen-20261019T084200Z/identities/trading-system` (JWT key, binding, env unchanged; `DATA_LAYER_V2_JWT_MANIFEST_REVISION=11`). Because the outage exceeds the writer's 120 s execution-surface TTL, start it once with `MARKET_DATA_NATIVE_REQUIRE_RESUME=0` (BOOTSTRAP) and, when ready, recreate it with the deployed `=1` (STRICT); a STRICT start after > 120 s parks every native TRADE/QUOTE slice in HANDOFF_RESUME_REQUIRED (rehearsal debug run, IDR-101 fail-closed) | BOOTSTRAP: demanded = ready; STRICT: demanded = ready = 64, 0 unhealthy, 0 auth/protocol failures (401, UNAUTHENTICATED, TLS verify, STRICT_RESUME_REFUSAL, HANDOFF_RESUME_REQUIRED) |
| W13 | Acceptance (section 8) | all PASS |

Expected durations: see section 7 (rehearsal). Consumer-visible outage = W1 start to W12 ready.

## 5. TS writer slot (W12-A default, W12-B optional)

Rule (coordinator, Codex review C, 2026-10-19): **W12-A is the default**: keep writer 2deb90b0 and switch only its
TLS identity mount. **W12-B** (TS release image with the group-commit and V1-lease changes) only if, before the
window, that image is final and has its affected correctness/performance proof (same-route overlapping ACKs, fsync
error, cancellation, queue saturation, interval 0 vs 50 commit/ACK latency) and the TS owner's go; it keeps its own
rollback (2deb90b0 + old identity) and its own evidence. The Data Layer does not wait for writer optimisation. The DL
side needs only: the new identity mount, JWT key unchanged, `DATA_LAYER_V2_JWT_MANIFEST_REVISION=11`, and the
BOOTSTRAP -> STRICT start of W12.

Also outside DL but on the old identity: the stopped containers `market_data_service`,
`market_data_service_p184w-dr07-rollback`, `market_data_service_p184w-r49-rollback` mount the 2026-08-22 identity;
after the window they are rollback targets only while that identity is valid (to 2026-11-20).

## 6. Rollback

Rollback restores the old certificates and the old digests; the old material is untouched by the window (the new
generation is a separate directory), so rollback is the same transaction in reverse, valid until 2026-11-20 14:43Z.

1. Stop the writer, then all DL mesh roles and the brokers (W1-W3).
2. Recreate with the v2.2.4 env and the deployed generation:
   `C_OLD="docker compose -p qdl_v2_stable_candidate -f deploy/compose/qdl-v2-stable-kn.yml --env-file deploy/compose/qdl-v2-stable-kn.v2.2.4.env --env-file ~/.local/state/qdl-v2/tls/gen-20260822T144323Z/generation.env --env-file ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env"`, same order W4, W6-W11.
3. Writer: its saved create compose (old image, old identity), BOOTSTRAP then STRICT as in W12.
4. K6: nothing to roll back (no alter in the window). The restore command for the pre-window state, if W5 had to
   remove a reappeared key, is `--apply-temp --apply` with the old generation's cert dir.
5. Last resort: the saved per-role inspect composes from step 2.4 recreate the exact pre-window definitions.

bar edge note: the held image a388537 bakes catalog r11 / acquisition r19, matching its checkpoint (catalog 11,
acquisition 19). A python image from this release bakes r12 / r20, and `stable_bar_edge._restore_state`
(`qdl/runtime/stable_bar_edge.py:560-567`) would refuse that checkpoint ("catalog_revision differs from runtime
authority"); `scripts/migrate_stable_bar_edge_checkpoint.py` cannot migrate a catalog revision. Rolling bar edge is
therefore not part of this window.

## 7. Rehearsal (isolated, 2026-10-19)

Receipt: `upgrade/evidence/releases/v2.2.5/rehearsal-20261019.json` (run 10:34-11:07Z, tool and compose at
3915a76, host lock claude-dl-v225, no foreign job). ISOLATED rehearsal on the same host: compose project
qdl-v225-reh from the committed compose with an overlay (own networks/volumes, no ports, CPU caps 2.99 vCPU in total,
Kafka super.users by CN instead of the production ACL list); kafka1-3, rust_core, both ingestors (live public
venue WebSockets, bindings narrowed to ETH perpetuals), market_projector_1, market_cache, stable_redis, query_kn_1,
stream_kn_1/2, and a TS writer clone (deployed image 2deb90b0 and env, ETH on both venues, 12 demanded slices).
Not production evidence; timings are under the caps and the narrowed data.

| Phase | Generation / images | Mesh stop | Brokers answer | DL roles healthy | Query READY (mTLS) | Writer BOOTSTRAP ready | **Consumer-visible outage** |
|---|---|---|---|---|---|---|---|
| A (baseline) | deployed 20260822T144323Z / v2.2.4 | - | - | - | READY | 12/12 | - |
| B rotate | new 20261019T084200Z / v2.2.5 | 18.7 s | 59.2 s | 159.0 s | 161.4 s | +47.1 s | **251.3 s** |
| C rollback | deployed / v2.2.4 | 20.9 s | 57.8 s | 137.2 s | 139.6 s | +82.8 s | **267.8 s** |
| D re-roll | new / v2.2.5 | 15.5 s | 59.5 s | 139.3 s | 141.8 s | +108.6 s | **290.7 s** |

(Times after the first writer stop; "DL roles healthy" and "Query READY" count from the recreate of the brokers.)

Per phase B, C, D:
- Trust anchor moved both ways: the Query and both Stream gateways refused the other generation's TS identity
  (CERTIFICATE_VERIFY_FAILED / unable to get local issuer), Kafka refused the other generation's admin identity and
  accepted the current one; the Kafka dynamic listener truststore override (truststore-kn-v220.jks) carried over.
- Offsets: every consumer group (core, projector stage A/B, both book-resync groups) present after the switch with no
  committed offset behind its pre-switch value; URP 0.
- Writer: clean stop exit 0; durable cursor items unchanged (TRADE 2 / QUOTE 2); BOOTSTRAP until 12/12 READY, then the
  deployed STRICT start: 12/12 READY at the steady check, 0 STRICT_RESUME_REFUSAL / HANDOFF_RESUME_REQUIRED; auth and
  protocol failures 0 (401, UNAUTHENTICATED, TLS verify) on the writer, Query and Stream; data-policy refusals
  counted apart (SOURCE_UNAVAILABLE MARK reference reads during start-up only).
- stable_redis recreated with the mesh; the writer was admitted again (quota state restarts by design).
- Teardown: 13 containers, 4 networks, 4 volumes, 4 work dirs removed; 0 left.

Findings that change the window procedure:
1. The writer's execution surfaces in its Redis expire 120 s after the last market row. A STRICT start after a longer
   outage parks every TRADE/QUOTE slice in HANDOFF_RESUME_REQUIRED (terminal for the process, IDR-101). W12 therefore
   starts the writer once in BOOTSTRAP and then in the deployed STRICT mode.
2. In this run the STRICT-restart timer accepted the stopped process' last heartbeat (false positive); STRICT readiness
   is shown by the steady sample about 60 s later. The tool is fixed for future runs.

Production expectation: the outage is the window's critical path; plan for 5-10 minutes from W1 to W12 READY (more
data per broker, 64 writer slices, 3 cores, 2 projectors and 2 Query replicas), with the rollback trigger in
section 9.

## 8. Acceptance after W12

1. Every role running and healthy, restart count 0; parity of the compose (v2.2.5 env + new generation) against
   live: PASS with only the intended deltas.
2. Trust anchor moved: Query and Stream refuse the OLD TS identity; Kafka refuses the OLD admin identity; the NEW
   ones succeed.
3. Writer: demanded = ready = 64, 0 unhealthy; 0 lines of 401 / UNAUTHENTICATED / CERTIFICATE_VERIFY_FAILED /
   STRICT_RESUME_REFUSAL / HANDOFF_RESUME_REQUIRED after the STRICT start; data-policy refusals (DATA_STALE,
   SESSION_STATE, EVENT_AGE, SOURCE_UNAVAILABLE) counted per feed with cause, separately; READY slices and
   execution-eligible views reported as two numbers.
4. Kafka: brokers 1,2,3, URP 0, core group 3 members, projector lag back to the pre-window range.
5. Certificate expiry: every mounted certificate notAfter >= 2027-10-19 (`openssl x509 -enddate` on public files).
6. 60 min passive soak (no load added): writer readiness and the passive latency snapshot (request latency
   histograms, served age) within the v2.2.4 certified envelope.

## 9. Owner go / no-go

- [ ] Date/time of the window (before 2026-11-06) and the window owner.
- [ ] v2.2.5 merged in dev, CI green; python image digest recorded.
- [ ] TS release image + equivalence proof ready, or fallback W12 (identity switch only) accepted.
- [ ] Pre-window checks 2.2-2.7 PASS on the day.
- [ ] Portal stacks paused for the window (IDR-128).
- [ ] Rollback trigger agreed: any W-step pass condition not met within 15 min, or writer not 64/64 within 15 min
      of W12.
- [ ] After acceptance: certificate scope per `CERTIFICATE_SCOPE.md` (same folder), then tag v2.2.5.

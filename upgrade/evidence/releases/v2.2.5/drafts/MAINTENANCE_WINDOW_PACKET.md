# Data Layer v2.2.5 maintenance window packet (DRAFT, not executed)

Status: DRAFT for the owner's go/no-go. Nothing in this packet has been executed on production. The procedure was
rehearsed on an isolated stack (section 7). Target window: before 2026-11-06; hard deadline 2026-11-20 14:43Z (expiry
of every deployed mesh certificate).

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
| Query image | qdl-v2-python:2.2.4-31a5155 (3e9e4d3a) | qdl-v2-python:2.2.5-<sha7> (digest in section 3) | query_kn_1/2 |
| Rust images | 27c79b53 (ingestors, stream), 60f83a51 (projectors), 7c848aad (core) | **unchanged digests** | recreated for mounts/healthchecks only |
| bar edge | qdl-v2-python:2.2.3-rc14-a388537 (08f356a8), held | **unchanged image** (held: a new python image bakes catalog r12 / acquisition r20 and would refuse the r11/r19 checkpoint, section 6) | recreated for mounts only |
| Kafka topic config | `__consumer_offsets` min.cleanable.dirty.ratio 0.05 (temporary, owner 2026-10-16) | broker default (K6) | no |
| TS writer | local/trading-system:p184-writer-release-30f50a91 (2deb90b0), identity 2026-08-22 | TS release image (slot, section 5), identity of the new generation | yes |

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
| query_kn x2 | qdl-v2-python@sha256:3e9e4d3a... | qdl-v2-python@sha256:PYTHON_V225 |
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
| W5 | K6: `python3 -B scripts/kafka_internal_topic_compaction.py --remove-temp --apply --cert-dir ~/.local/state/qdl-v2/tls/gen-20261019T084200Z/kafka --lock-owner <window owner>` | describe shows no `min.cleanable.dirty.ratio` override on `__consumer_offsets`; others unchanged |
| W6 | `$C up -d --no-deps --force-recreate rust_core rust_core_2 rust_core_3` (simultaneous: same image, no mixed-version group) | 3 members in `qdl-v2-realtime-core-v2`, healthy, no retry loop |
| W7 | `$C up -d --no-deps --force-recreate ingestor_binance_usdm ingestor_okx_swap` | healthy (session-liveness fresh) |
| W8 | `$C up -d --no-deps --force-recreate market_projector_1 market_projector_2` | healthy (status fresh), lag falling |
| W9 | `$C up -d --no-deps --force-recreate query_kn_1 query_kn_2` | healthy; `/health/ready` READY over mTLS with the NEW TS identity; ready products back to the baseline |
| W10 | `$C up -d --no-deps --force-recreate stream_kn_1 stream_kn_2` | healthy; TLS handshake with the NEW TS identity verifies |
| W11 | `$C up -d --no-deps --force-recreate binance_bar_edge` (image unchanged) | healthy; log shows checkpoint restore, no bootstrap |
| W12 | TS writer recreate: TS release image, `/run/secrets/qdl-v2` -> `~/.local/state/qdl-v2/tls/gen-20261019T084200Z/identities/trading-system` (JWT key, binding, env unchanged; `DATA_LAYER_V2_JWT_MANIFEST_REVISION=11`) | heartbeat demanded = ready = 64, 0 unhealthy, 0 401 / UNAUTHENTICATED / CERTIFICATE_VERIFY_FAILED |
| W13 | Acceptance (section 8) | all PASS |

Expected durations: see section 7 (rehearsal). Consumer-visible outage = W1 start to W12 ready.

## 5. TS writer slot (owned by the TS release agent)

The writer is rolled to the TS release image in W12. Required before the window: the TS release tag/image digest
and its closure-equivalence proof against the deployed writer 2deb90b0 (same env contract, same binding, native
writer behaviour), and the TS owner's go. If the TS release is not ready, W12 recreates the current writer image
2deb90b0 with only the identity mount switched (its saved create compose with the one mount source changed) and the
TS roll moves to its own later change. The DL side needs only: the new identity mount, JWT key unchanged,
`DATA_LAYER_V2_JWT_MANIFEST_REVISION=11`.

Also outside DL but on the old identity: the stopped containers `market_data_service`,
`market_data_service_p184w-dr07-rollback`, `market_data_service_p184w-r49-rollback` mount the 2026-08-22 identity;
after the window they are rollback targets only while that identity is valid (to 2026-11-20).

## 6. Rollback

Rollback restores the old certificates and the old digests; the old material is untouched by the window (the new
generation is a separate directory), so rollback is the same transaction in reverse, valid until 2026-11-20 14:43Z.

1. Stop the writer, then all DL mesh roles and the brokers (W1-W3).
2. Recreate with the v2.2.4 env and the deployed generation:
   `C_OLD="docker compose -p qdl_v2_stable_candidate -f deploy/compose/qdl-v2-stable-kn.yml --env-file deploy/compose/qdl-v2-stable-kn.v2.2.4.env --env-file ~/.local/state/qdl-v2/tls/gen-20260822T144323Z/generation.env --env-file ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env"`, same order W4, W6-W11.
3. Writer: its saved create compose (old image, old identity).
4. K6 rollback is not needed (the default ratio is the pre-2026-10-16 state); if wanted: `--apply-temp --apply`.
5. Last resort: the saved per-role inspect composes from step 2.4 recreate the exact pre-window definitions.

bar edge note: the held image a388537 bakes catalog r11 / acquisition r19, matching its checkpoint (catalog 11,
acquisition 19). A python image from this release bakes r12 / r20, and `stable_bar_edge._restore_state`
(`qdl/runtime/stable_bar_edge.py:560-567`) would refuse that checkpoint ("catalog_revision differs from runtime
authority"); `scripts/migrate_stable_bar_edge_checkpoint.py` cannot migrate a catalog revision. Rolling bar edge is
therefore not part of this window.

## 7. Rehearsal (isolated, 2026-10-19)

REHEARSAL_RESULTS

## 8. Acceptance after W12

1. Every role running and healthy, restart count 0; parity of the compose (v2.2.5 env + new generation) against
   live: PASS with only the intended deltas.
2. Trust anchor moved: Query and Stream refuse the OLD TS identity; Kafka refuses the OLD admin identity; the NEW
   ones succeed.
3. Writer: demanded = ready = 64, 0 unhealthy; 0 lines of 401 / UNAUTHENTICATED / CERTIFICATE_VERIFY_FAILED /
   STRICT_RESUME_REFUSAL; strict resume from committed cursors (no bootstrap).
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

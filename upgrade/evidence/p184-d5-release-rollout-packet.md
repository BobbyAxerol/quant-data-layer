# P18.4 D5: Data Layer v2.2.3 Release Rollout Packet

Status: DRAFT. Digests are filled in after the one-commit build. Each runtime step needs owner approval of this
exact blast radius and rollback.

## Goal

Close IDR-039: every application role of `qdl_v2_stable_candidate` runs images built from ONE commit of
`fix/p184-dl-closure`. That commit carries:
- IDR-062 (BOOK_SNAPSHOT on top-of-book change);
- IDR-040 fix 1 (REST_AHEAD_OF_STREAM) and fix 2 (quiet-book proof, ingestor flag default off);
- IDR-082 (`event_id` on the view);
- IDR-084 and the rc4 Query fixes (MARK single-flight, env cold slice);
- the SDK 2.2.0 transport fix.

## Inventory before the rollout (2026-10-10 04:4xZ, `docker inspect`)

| Role | Image ID | Label revision |
|---|---|---|
| query_kn_1, query_kn_2 | sha256:5a9e1b4b8be1 (2.2.3-rc4) | bd74faa |
| binance_bar_edge | sha256:514506122111 (2.2.2) | d6d2637 |
| ingestor_binance_usdm, ingestor_okx_swap | sha256:0f6876e16e51 | 855c914 |
| rust_core, rust_core_2, rust_core_3 | sha256:658a9570c5fc | 0e00c81 |
| market_projector_1, market_projector_2 | sha256:a696cfba6ab3 | c8015ca |
| stream_kn_1, stream_kn_2 | sha256:1f13408fe946 | c8015ca |

Not touched by this packet:
- kafka1-3 (sha256:9516fb7634ba);
- market_cache and stable_redis (redis@sha256:dfa188...);
- the one-shots stable_state_init and stable_tls_init;
- the TS writer `market_data_service_p184w`;
- the realm dirs and consumer manifests (rev11 TS, rev15 alpha);
- every env value except the image, including QDL_KN_MAX_SUBSCRIPTIONS=1024 and any tuned QDL_QUERY_COLD_SLICE_MS.

## Build (one commit, both images)

- Source: `git archive <release-sha>` into a private dir. One archive feeds both Dockerfiles (`Dockerfile` and
  `Dockerfile.qdl-rust-runtime`), following scripts/p184_consolidated_build.py.
- Tags: `qdl-v2-python:2.2.3-<sha7>` and `qdl-v2-rust:2.2.3-<sha7>`. Record the image IDs and label revisions;
  the revision must equal `<release-sha>`.
- Before any recreate, check that every Rust role's current command/entrypoint binary exists in the new Rust image
  (`docker run --rm --entrypoint ls <rust-image> <binary path>`).
- Local exact-source CI equivalent of `.github/workflows/ci.yml` on the same archive. The DL repo is not pushed, by
  owner rule. Record the receipt.

## Rollout order (one role at a time; next role only after health + guard green)

Guard during every step: TS writer READY 64/64, RestartCount and OOM of the changed role, Kafka ISR, disk >= 20 GB.

1. query_kn_1, then query_kn_2: python release image. Wait for healthy.
2. binance_bar_edge: python release image. Coupling C.19/C.32: bar edge reads its acquisition plan from the image.
   Compare the plan revision in the old and new image first; if it differs, STOP (it would strand the checkpoint).
3. stream_kn_1, then stream_kn_2: Rust release image. Expect TS stream reconnects (G11 bound).
4. market_projector_1, then market_projector_2. G12 isolated: 0.26 s caught up.
5. rust_core, rust_core_2, rust_core_3, one at a time. Wait for quorum and READY each time. Books reset and TS sees
   typed gaps (DR05 path).
6. ingestor_okx_swap, then ingestor_binance_usdm: provider sessions reconnect. `book_liveness_proof_ms` stays unset
   (IDR-040 fix 2 off).

## Rollback

Per role, recreate from the saved live compose (from `docker inspect` before its step) on the previous image ID in
the inventory above. Roll back in reverse order. No data migration: Kafka, Redis and the realm dirs are untouched.

## Acceptance after the rollout

- G13: DR01-DR08 on the final digests.
- G1-G14 scored on the final digests. G4/G5 use the frozen `config/v2/p184-stage100-budget.json`.
- Then release notes, digests, freeze. Known limits, recorded rather than release-blocking:
  - G12 was isolated: same host, a single replica, BOOK only.
  - IDR-040 fix 2 is off; ping cadence and marker volume are not measured.

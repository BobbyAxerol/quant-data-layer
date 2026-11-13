# DL v2.3.0: what must be filled after the window and the certificate

Refreshed 2026-11-13 after pre-flight F1/F2: base is the rc3 integration tip `3d8ccd7`; the release runtime is rc3
(rust `58bdd282`, python `a235c0a1`). Items marked DONE are already in the tree.

Fill everything before the single release CI run (CI_PLAN.md step 1). Each item names its producer.

## From rc3 window A (integration agent / coordinator)

1. Window receipts: non-secret byte copies plus `SHA256SUMS`, under `upgrade/evidence/releases/v2.3.0/window-<UTC>/`.
   - R-gates: roles healthy and RestartCount, compose parity, lineage/cursor gates D3/D4, D6b gap gate, section 7a
     CPU gate (DL vCPU <= 6.0), writer gates W.
2. Deployed digests from `docker inspect`, written into RELEASE_NOTES "Runtime":
   - rust `58bdd282...0512` and python `a235c0a1...d0b` (rc3, `qdl-v2-python:2.3.0-rc3-7cd4fc36`); pre-filled from
     docker inspect at about 16:10Z on 2026-11-13; reconfirm at fill.
   - The linger fix is config-only, so on its own it does not change the images. A changed image digest means a new
     image receipt and an updated table.
3. DONE (F2): the deployed compose and env are in the tree, byte-equal to the host copies:
   `deploy/compose/qdl-v2-stable-kn.yml` sha256 `5b1381e0...`, `deploy/compose/qdl-v2-stable-kn.v2.3.0.env`
   (= host `qdl-v2-stable-kn.v2.3.0-rc3.env`) sha256 `2b1b4b8d...`. Still to do: record both in the ledger row, and
   re-diff against the host copies at fill time (any later recreate that changes them invalidates this).
4. The declaration that went live:
   - DONE (F2): out6 `repo/` set committed (`config/v2` acquisition r22 with pins, catalogue r14, demand r10, scope
     r13, routing r31, two universes; `consumers/stable/alpha-okx-paper.yaml` r16). The mounted `runtime/*`
     documents stay host-only, with the reason recorded in RELEASE_NOTES "Release tree = runtime";
   - out6 digests (packet section 3); byte readback of the root-only `v2.3.0-rc2` and `v2.3.0-rc2-ingestor` dirs needs
     sudo (an operator step);
   - keep the render inputs record (`render-inputs.json`);
   - copy the declaration file into the evidence folder;
   - bar-edge migration record `cd15ee73...`, plus the post-window checkpoint schema v5 readback.
5. The bootstrap result: measured duration against the 72.7 min estimate, and the D6b gap gate verdict.

## From the certificate (R-cert owner)

6. R-cert 2 final verdict: run 2 is the weekend run on the rc3 images (namespace `qdl-v230-rcert2-20261113t1551z`,
   started 2026-11-13 15:51Z). The v2.3.0 release uses tonight's certificate (owner overnight package
   2026-11-13). The Monday 00:00Z crossing gates only the 1W/2D/3D derived declarations, which stay off until a
   Monday-crossing run passes. Run 1 (rc1 images) does not count.
7. R-cert 3 fault-matrix receipt; R-cert 4 capacity envelope, or the recorded window substitute.
8. `upgrade/evidence/releases/v2.3.0/certificate.json` (status PASS), `endpoint-report.json` and its sha in
   `certificate.endpoint_report_sha256` (checked by `release.yml`), and `CERTIFICATE.md`.
9. Move `drafts/RELEASE_NOTES.md` to `RELEASE_NOTES.md` and replace every `TBD`.

## Repository (release-package author)

10. Fill the `CERTIFICATION_LEDGER.md` v2.3.0 placeholders: R-cert 2, window, certificate.
11. Plan journal entry for the release package (AGENTS.md 5-8) and the guide delta.
12. `scripts/build_qdl_release_images.py` `RELEASE = "2.3.0"` is already bumped. rc3 Python was cut: record the
    `a235c0a1` build receipt (git archive of `7cd4fc36`, dependency `753604f8`) in the ledger.
13. The PR body (`drafts/PR_BODY.md`): replace the TBD lines. Then one PR into dev, as CI_PLAN.md says.

## Not part of v2.3.0 (do not fill here)

- Stage B (S1-S4, TS QUOTE/MARK for the remaining 69 crossec swaps): after the v2.3.0 release, as a later declaration.
  Re-render it on the measured post-arm baseline, with the owner disk floor (>= 14-15% free, never below 10%;
  it does not block the main flow: flag it so the owner adds disk, planned 2026-11-14/15) and its own packet that includes the
  multi-member group proof.
- The 4-contract `price_tick` refresh: a later declaration with its own migration record.
- Live funding binding (v2.3.1), DL-BL-2026-11-13-E (v2.3.1), per-row bootstrap depth (DL-BL-2026-11-13-C).
- v2.2.7 (`13f20d0`, branch `release/v2.2.7-ingestor-linger`): decided, not tagged; superseded by v2.3.0, not
  released.
- CPU optimisations (projector commit coalescing, fetch batching, pipelined Redis reads, stream ListOffsets) and live
  funding: v2.3.1. The v2.3.0 digests are 58bdd282 / a235c0a1.
- Linger / broker fetch: runtime config only. Record the chosen values in RELEASE_NOTES "Runtime" only if their
  measurement arms pass.

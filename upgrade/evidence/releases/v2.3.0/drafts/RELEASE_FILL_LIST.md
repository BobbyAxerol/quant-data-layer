# DL v2.3.0: what must be filled after the window and the certificate

Fill everything before the single release CI run (CI_PLAN.md step 1). Each item names its producer.

## From the combined window (integration agent / coordinator)

1. Window receipts: non-secret byte copies plus `SHA256SUMS`, under `upgrade/evidence/releases/v2.3.0/window-<UTC>/`.
   - R-gates: roles healthy and RestartCount, compose parity, lineage/cursor gates D3/D4, D6b gap gate, section 7a
     CPU gate (DL vCPU <= 6.0), writer gates W.
2. Deployed digests from `docker inspect`, written into RELEASE_NOTES "Runtime":
   - rust `58bdd282...0512` and python `03de529a...441b` stay, unless an rc3 is cut.
   - The linger fix is config-only, so on its own it does not change the images. A changed image digest means a new
     image receipt and an updated table.
3. The deployed compose and env:
   - add `deploy/compose/qdl-v2-stable-kn.v2.3.0.env` = the env the window ran (images by digest), byte-equal to the
     host copy;
   - record its sha256 in the ledger row.
4. The declaration that went live:
   - out6 digests (packet section 3);
   - keep the render inputs record (`render-inputs.json`);
   - copy the declaration file into the evidence folder;
   - bar-edge migration record `cd15ee73...`, plus the post-window checkpoint schema v5 readback.
5. The bootstrap result: measured duration against the 72.7 min estimate, and the D6b gap gate verdict.

## From the certificate (R-cert owner)

6. R-cert 2 final verdict (S1-S6, Monday 06:00Z run; final after 12:30Z). Re-run on the rc2 images if the owner
   requires it.
7. R-cert 3 fault-matrix receipt; R-cert 4 capacity envelope, or the recorded window substitute.
8. `upgrade/evidence/releases/v2.3.0/certificate.json` (status PASS), `endpoint-report.json` and its sha in
   `certificate.endpoint_report_sha256` (checked by `release.yml`), and `CERTIFICATE.md`.
9. Move `drafts/RELEASE_NOTES.md` to `RELEASE_NOTES.md` and replace every `TBD`.

## Repository (release-package author)

10. Fill the `CERTIFICATION_LEDGER.md` v2.3.0 placeholders: R-cert 2, window, certificate.
11. Plan journal entry for the release package (AGENTS.md 5-8) and the guide delta.
12. `scripts/build_qdl_release_images.py` `RELEASE = "2.3.0"` is already bumped. If an rc3 is cut, record its digests
    and build receipt.
13. The PR body (`drafts/PR_BODY.md`): replace the TBD lines. Then one PR into dev, as CI_PLAN.md says.

## Not part of v2.3.0 (do not fill here)

- Stage B (TS QUOTE/MARK for the remaining 69 crossec swaps): a later declaration, not a release.
- The 4-contract `price_tick` refresh: a later declaration with its own migration record.
- Live funding binding and per-row bootstrap depth (DL-BL-2026-11-13-C): v2.3.x candidates.
- v2.2.7 (`13f20d0`, linger hotfix) is not an ancestor of rc2. Its knob was re-implemented in slice 2a/2b. Whether
  v2.2.7 is tagged separately is an owner decision.

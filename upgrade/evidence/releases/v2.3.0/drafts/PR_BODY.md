## Summary

Data Layer **v2.3.0**: coverage becomes a declaration. Adding instruments or intervals becomes render a declaration,
pass the offline gate, ordered restart; no campaign, image or tag. DL-side derived bars are added.

- DERIVED_FINAL bars aggregated from final 1m inside the standard path. The Rust core stays the only canonical writer
  (`BAR_ORIGIN_AGGREGATED`); grouping is anchor-aware.
- Declaration mechanism:
  - consumer-manifest accept-set (`min_token_revision`);
  - universe selectors;
  - effective-declaration provenance and guards;
  - bar-edge checkpoint v5 with migration records;
  - sticky ingestor lanes and per-feed flush;
  - mounted declarations in compose;
  - producer knobs, shipped unset.
- `scripts/dl_declaration.py`: `render` / `gate` / `checkpoint-migration`.
  - Adapters for TS `ts.declared-execution-requirements.v1` and alpha hosting declarations.
  - Quotas from the quota model, a declarable final-BAR source, the OKX REST budget gate, a bar-edge runtime pair,
    and the 6.0 vCPU OKX-first cap.
- Per-binding contract lineage and cursor lineage, seeded from the live ingestor stamps; contract-line `schema_digest`.
- v2.2.6 stage A configuration (catalogue r13, TS Paper r13 with model quotas).
- SDK 2.5.0: additive; the bundled generated contract gains `Bar.aggregation` (no SDK source change).

Release notes: `upgrade/evidence/releases/v2.3.0/RELEASE_NOTES.md`; certificate: `.../certificate.json` (TBD PASS).

## Validation

- R-cert 1 offline derived-bar parity: PASS, 740,350 records, 0 mismatches (receipt `6814e25c`).
- R-cert 2 live shadow: TBD final verdict; run 2 is a weekend run on the rc3 images (started 2026-11-13 15:51Z).
- R-cert 3 fault matrix: TBD. R-cert 4 capacity envelope: not run; window substitute (owner delta gate) PASS, 6.54 <= D0 6.97 + 0.3.
- Combined production window (rc2 `832c02c4` + TS writer W-B2), 2026-11-13: D1-D6 PASS, W1 VERIFY_FAILED, rolled
  back (`window-20261113T1237Z/`). rc3 window A (DL only, Python `a235c0a1`): GREEN, receipt `window-A-20261113T1444Z/` (D1-D6, D5a, writer W1/W2, D6b + 15 min, CPU gate 6.54 <= 7.27).
- rc2 full suite on `269fc9c8`: 4,296 tests, 4 environment-only errors (non-root log permission; 15 OK as root).
- rc3 full suite: 4,304 tests, 4 environment-only errors, root re-run 15 OK (packet section 0b, `3d8ccd7`).
- Release tree = runtime (F2): the deployed compose and rc3 env byte-equal; the live declaration out6 as evidence
  (`upgrade/evidence/releases/v2.3.0/declaration-out6/`); `config/v2` stays the test baseline. Impacted suite on this tree: 1352 OK (63 skipped), 96 modules, 79bbfb79, rc3 image, 1 CPU (3369 s).
- Delta tests on tool `5a42ba01`:
  - `test_v230_declaration_tool`, `test_v230_bar_source`, `test_v230_lineage`: 47 OK, including the env-gated stage A
    goldens;
  - one named container, 1 CPU.
- First production declaration: gate 18/18 PASS (render out6).
- Expected CI: **one full run** on this PR (CI-defining paths changed since the last full green `098c94b`):
  - jobs: changes, repository-scan, consolidated-source-preflight, contract-tests, rust-workspace, unit-tests x4,
    sdk-python310, release-gates, kn-native-integration, ci-result;
  - about 56.5 runner-minutes on `ubuntu-latest`, about 13 min wall.
  - The dev push and the dev -> main PR/push inherit by tree identity.
  - The `v2.3.0` tag runs `release.yml` once (about 5 min).

## Risk And Rollback

- Production runs rc3 (rust `58bdd282`, python `a235c0a1`); this PR carries the same code, the deployed compose/env and
  the out6 configuration, plus evidence.
- Rollback of the deployment: window packet section 5. Back to v2.2.6 stage-A r13 / stage-A / v2.2.5 images and
  composes, restoring the pre-rollout bar-edge checkpoint backup (v4; the old image cannot read v5). The writer goes
  BOOTSTRAP then STRICT (W.5).
- A declaration rollback is a forward declaration, so revisions stay monotonic.
- Projector rollback constraint (packet amendment 4'/6'): the target projector `60f83a51` refuses derived bars
  (`ENVELOPE_NOT_CANONICAL`, R-cert 2 S5) and would crash-loop, so the projectors stay on rc3 `58bdd282` while any
  derived record exists on `md.bars.v2`; only Query, stream, cores, bar_edge and the ingestors roll back. New
  rollback-receipt gate: no `ENVELOPE_NOT_CANONICAL`, RestartCount unchanged 10 min, `bars_applied` rising.
- Known issue: a broker leadership move can restart a projector once, about 45 s, exactly-once kept (IDR-285, fix in
  v2.3.1).
- Known limits: single host; IDR-128 open; funding is reference-only here (live binding in v2.3.1); bootstrap depth is
  manifest-wide; bootstrap and gap repair share the HISTORY_CANDLES lane; 1W/2D/3D derived bars off until R-cert 2
  passes a Monday boundary.
- Not touched: the TS repository (W-B2 is a TS release), Kafka topics and retention, the TLS generation.

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
- SDK unchanged (2.4.1).

Release notes: `upgrade/evidence/releases/v2.3.0/RELEASE_NOTES.md`; certificate: `.../certificate.json` (TBD PASS).

## Validation

- R-cert 1 offline derived-bar parity: PASS, 740,350 records, 0 mismatches (receipt `6814e25c`).
- R-cert 2 live shadow: TBD final verdict (interim 840/840 MATCH on rc1).
- R-cert 3 fault matrix: TBD. R-cert 4 capacity envelope: TBD.
- Combined production window (rc2 `832c02c4` + TS writer W-B2), 2026-11-13: TBD, receipts under
  `upgrade/evidence/releases/v2.3.0/window-*`.
- rc2 full suite on `269fc9c8`: 4,296 tests, 4 environment-only errors (non-root log permission; 15 OK as root).
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

- Production already runs rc2 after the window; this PR carries the same code and configuration, plus evidence.
- Rollback of the deployment: window packet section 5. Back to v2.2.6 stage-A r13 / stage-A / v2.2.5 images and
  composes, restoring the pre-rollout bar-edge checkpoint backup (v4; the old image cannot read v5). The writer goes
  BOOTSTRAP then STRICT (W.5).
- A declaration rollback is a forward declaration, so revisions stay monotonic.
- Known limits: single host; IDR-128 open; funding is reference-only (no live binding); bootstrap depth is
  manifest-wide; bootstrap and gap repair share the HISTORY_CANDLES lane; 1W/2D/3D derived bars off until R-cert 2
  passes a Monday boundary.
- Not touched: the TS repository (W-B2 is a TS release), Kafka topics and retention, the TLS generation, the SDK wheel.

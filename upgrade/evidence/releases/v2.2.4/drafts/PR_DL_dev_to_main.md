# DL PR draft 2/2: release PR `dev` -> `main`, then tag `v2.2.4` (DRAFT, not opened)

Status: draft written 2026-10-17. Opened only after PR 1/2 is merged into `dev`, the certificate is PASS, required CI is
green on the `dev` head, and the owner asks for the release (CLAUDE.md G6; FINAL_STEPS h). Same governance note as PR
1/2: no validator in the DL repo; `CONTRIBUTING.md` requires the release PR `dev` -> `main`.

## Title

`chore(release): Data Layer v2.2.4 P18.4 phase D closure`

(Precedent: "Release v2.2.3: P18.4 phase D Data Layer closure". The conventional form is used so the TS `TITLE_RE`
also accepts it.)

## Body

```
## Summary

Release PR for Quant Data Layer v2.2.4 (previous stable v2.2.3, tag on 28e605e). Merges dev (which carries the
fix/p184-dl-closure release candidate, PR <<PENDING:DL_PR_NUMBERS_AND_TAG>>) into main.

Certificate: upgrade/evidence/releases/v2.2.4/certificate.json, status <<PENDING:CERTIFICATE_STATUS>>, sha256
<<PENDING:CERTIFICATE_JSON_SHA256>>. Endpoint report sha256 <<PENDING:ENDPOINT_REPORT_SHA256>>. SDK qdl-sdk 2.4.1 wheel
sha256 9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7 (release.yml rebuilds it from the tag and
compares). Source c7ce7d3087cb8a2daf82ba941982bd296d488f45; release head <<PENDING:DL_RELEASE_HEAD_SHA>>.

Gates: <<PENDING:SUMMARY_PASS>> PASS, <<PENDING:SUMMARY_FAIL>> FAIL, <<PENDING:SUMMARY_NOT_MEASURED>> NOT_MEASURED,
<<PENDING:SUMMARY_DEFERRED>> DEFERRED (SCORECARD.md). G12 PASS credited on the final writer a44349a1; G13 76/76 (isolated 12/12,
runtime window v5.4 closes 76/76); stage-100 <<PENDING:S100_DIR>>.

Limits stated in the certificate: single host (not HA); binance_bar_edge held on rc14; rust_core x3 on rc22 by binary
equality; build receipt FAIL resolved by the postcheck addendum; Kafka C converging; IDR-108 open; G12 harness shape;
TEST_ONLY burst results are not capacity claims.

## Type

- [x] Data contract / API
- [x] Docs / repo hygiene

## Checks

- [x] I did not commit directly to `main`.
- [x] Docs are updated for public behavior changes.
- [x] Tests are added or updated.
- [x] Docker tests pass, or a blocker is documented: <<PENDING:DL_LOCAL_CI_SUMMARY_SHA256>>
- [x] No secrets, generated logs, parquet data, or local caches are included.

## Test Evidence

Required CI on the dev head: <<PENDING:DL_CI_RUN_ID_DEV_HEAD>>. Release evidence directory
upgrade/evidence/releases/v2.2.4/. After merge: annotated tag v2.2.4 on the main merge commit; release.yml verifies
main ancestry, certificate status PASS, endpoint-report sha256 and the SDK wheel sha256, then publishes the GitHub
release with certificate.json, endpoint-report.json, the wheel, release.json and the SBOM.

Rollback: per-role digests and saved composes in RELEASE_NOTES.md (query b9ca6c36, stream baa72c96, projector
ce629061, ingestor 7fba2577); the release PR itself changes no running container.
```

## Tag (after the main merge; owner request)

```
git tag -a v2.2.4 <main-merge-sha> -m "Quant Data Layer v2.2.4: P18.4 phase D closure.

<<PENDING:TAG_MESSAGE_SCOPE_ONE_LINE>> (certificate upgrade/evidence/releases/v2.2.4/certificate.json); SDK 2.4.1 wheel 9f79e032."
```

Precedent message (v2.2.3): "Quant Data Layer v2.2.3: P18.4 phase D closure. Scoped certificate PASS on source 481fd67 ...
SDK 2.3.0 wheel b9c74157." Push the tag only on the owner's request.

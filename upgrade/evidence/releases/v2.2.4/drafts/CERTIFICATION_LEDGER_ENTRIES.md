# CERTIFICATION_LEDGER.md entries for v2.2.4 (DRAFT, not yet appended)

Status: draft text to append to `/CERTIFICATION_LEDGER.md` (after the `### 2026-10-12 - DL takeover reconciliation`
section) in the same commit as the final certificate (FINAL_STEPS g.4; ledger rule G4). Nothing here is a certificate.
Every `<<PENDING:...>>` is filled from the receipts named in [../SCORECARD.md](../SCORECARD.md). Hashes below were
computed with `sha256sum` on 2026-10-17 unless marked otherwise.

The ledger's own rule applies: read it before re-testing; re-run only when the commit, image or config in the "Pinned
at" column changes.

---

### 2026-10-16 - v2.2.4 release preparation packet (not a certificate)

Pinned at: DL `fix/p184-dl-closure` `ef6e24b` (packet), updated through `c7ce7d3`; SDK 2.4.1 bump `aaa81a8`; image
recipe `46f4628` (`scripts/build_qdl_release_images.py`, script sha256 `52df319ee9b294cdc16821156ff80ed29cdd4adf29afa14cb63347c995ee28f7`).

Status: PREPARED. The packet holds the change map, the proof map per component, blockers B1-B20 and the ordered steps
([FINAL_STEPS.md](upgrade/evidence/releases/v2.2.4/FINAL_STEPS.md)). The inheritance audit `badee80` classified G12 and G13
by scenario and cell; no v1-predicate G12 PASS is credited. Prior v2.2.3 certificate and notes are immutable (the 8 MiB/s
cleaner statement in them is corrected in the v2.2.4 notes, not rewritten).

### 2026-10-16 - v2.2.4 release images built and rolled (build receipt FAIL, resolved by addendum)

Pinned at: REV `c7ce7d3087cb8a2daf82ba941982bd296d488f45`; rust `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e`;
python `sha256:c21e164ac4db14af498b73104e344f003fa7117fdae2e465f50df8668cdca252`; SDK wheel
`9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7`.

- Build receipt `~/.local/state/qdl-v2/build-v2.2.4-c7ce7d3/receipt.json` `40d727b28a3a581eb9c5911e150a39ae0dac68d8d0a9afbb5cceee47b4cb4f7f`:
  status **FAIL**, kept. Two whole-file expectations failed. Addendum `dad31fe8dc7f768de72e7577951af61d726689d8b32fcf5153046fd9bb190260`
  (coordinator, owner-delegated, 2026-10-16): projector EQUAL_LOADED_SECTIONS to rc22; gateway DIFFERENT_BUILD_PATH with a
  DR08 live-budget isolation A/B PASS (prereg `d22561fe...`, verdict `137ab779...`).
- Production roll 2026-10-16 17:14-17:53Z, one container at a time: query x2, stream x2, projector x2, ingestor x2.
  Summary `eefb041c5ece53c17d28148eb3fa1c2e6be64f84b79ab20e6442f1bd44c0b271`. RestartCount 0 on every role; rust_core x3 kept
  on rc22 (`qdl-realtime-core` EQUAL); bar edge held.
- Status: tested locally / rolled. Not certified until the scorecard closes.

### 2026-10-16 - Kafka runtime config B + C + A' applied

Pinned at: kafka image `sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9` (unchanged); effective
describe `d3d77176c3dfca46e7f44cba465d271afe2709a1fc9d97f75e808808fa959094` (2026-10-17T09:16:16Z, schema v2).

- B: `auto.leader.rebalance.enable=false` x3 (kafka1 14:14Z, kafka2 14:21Z, kafka3 14:25Z); C: `__consumer_offsets`
  `delete.retention.ms=600000` + `segment.ms=60000`, `__transaction_state` `segment.ms=600000`; A': `md.latest.v2`
  `delete.retention.ms=21600000` (02:05:37Z); cleaner 4 MiB/s since 2026-10-14.
- First production coordinator load after the cleaner pass: 4,783 ms
  (`coordinator-loads.json` `a7c0a8661482a4c30ebc303c8d9d4992c62e60a9cf317ddc20c3ed78dd26f473`).
- Convergence of C is estimated at ~49 h (not measured to the end). Rollbacks are listed in the v2.2.4 notes.

### 2026-10-17 - G12 final credit on the final writer a44349a1

Pinned at: DL `f6db0b82`; image set `g12-image-set-v224.json` `4740c995550737e7d30325aaa043f07cf3656014ca5611dd97e482619504ca61`;
writer `sha256:a44349a10e5284efae290ea38062dd23dea0ef015088be6cf041d3f4d0923c73` (TS `6af62a7e`); Kafka effective
`d3d77176...`.

- Receipt `p184-g12-v224-final-a44349a1-20261017T084728Z` (`evidence/verdict.json` `30648759e5766f5e085e1ea3874efe2ab69ceca0475fcca2ab23e3016b67d1be`):
  verdict v3 `pass: true`; `--credit` exit 0, `production_parity` MATCH. caught_up_s BINANCE/OKX: broker 0.217/0.218,
  projector 0.174/0.174, Stream 0.759/0.760.
- Level: **shadow-certified** (isolated TEST_ONLY harness; one host). Inherited from earlier digests: nothing.
- Caveats: WriterClone apparatus pin `77a6d671` (H1, deferred); integrity layer report-only; harness shape differs from
  production (core members, Kafka CPU, topic segment settings).

### 2026-10-17 - G13 isolated 12/12 and runtime window 76/76

Pinned at: DL run worktree `f6db0b82`; TS `6af62a7e`; roles `g13-roles-v224.json` `31e0716b04f05ee156b0527b8905cf3cb3487501a9a4566f0742b80664342991`.

- Isolated (writer a44349a1): dr08 4/4 (`T091825Z`), dr05-upstream 6/6 (`T093001Z`, after one NOT SCORED apparatus
  rerun `T092050Z`), dr05-quiet 2/2 under amendment-1 (`T092651Z`, predicate
  `p184.dr05-quiet-typed.view-vocabulary.v1`).
- Runtime v5.3 `T094028Z`: 67/76, STOPPED at dr05-transport (IDR-111 apparatus). Runtime v5.4:
  `p184-g13-20261017T103357Z` (summary `9d22e11100e8f23ce76f4b79242ac02455ea7ad2097a7e57cd11436738c68367`), W05 `5490c21e66bf33bae326f2ea2051677f70cf95372b13b823d4c2912835da9895`: 76/76 DR cells closed; verifier exit 1 only from 260 CASE_OPEN cells
  outside G13, 0 DR issues. Packet `packet-g13-v5.4-20261017.md` `872265593331a0cc51ef3abb0f301bfdcb45a3978c5d4f845608813a0236101d`; run ids `g13r7wa20261017` / `g13r7fx01`;
  TS CI run `39024582832` success on `c9ebd619` (per the packet); harness fix `fc644cd1` = IDR-111, apparatus only.
- Level: **shadow-certified** on the release set, 76/76 (isolated cells are shadow-certified; runtime cells run against a TS stack on the
  production writer).

### 2026-10-17 - final stage-100 on the v2.2.4 release digests

Pinned at: DL `c7ce7d3` digests above, writer `a44349a1`, apparatus `<<PENDING:S100_APPARATUS_PINS>>`.

- Receipt `<<PENDING:S100_DIR>>`, scorecard `<<PENDING:S100_SCORECARD_SHA256>>`, `protocol_met`
  `<<PENDING:S100_PROTOCOL_MET>>`.
- Gates: `<<PENDING:SUMMARY_PASS>>` PASS; `<<PENDING:SUMMARY_FAIL>>` FAIL; `<<PENDING:SUMMARY_NOT_MEASURED>>`
  NOT_MEASURED; `<<PENDING:SUMMARY_DEFERRED>>` DEFERRED.
- History kept, not credited: stage-100 #3 (2026-10-16 02:45Z, protocol_met false) and the attempt of 2026-10-17
  03:20Z on writer 7c71e3af (protocol_met false; G9 and G4 FAIL; the writer was replaced afterwards).
- Level: `<<PENDING:S100_LEVEL>>`.

### 2026-10-17 - v2.2.4 certificate

`<<PENDING:CERTIFICATE_STATUS>>`. Certificate [certificate.json](upgrade/evidence/releases/v2.2.4/certificate.json) sha256
`<<PENDING:CERTIFICATE_JSON_SHA256>>`; endpoint report sha256 `<<PENDING:ENDPOINT_REPORT_SHA256>>`; PR / tag
`<<PENDING:DL_PR_NUMBERS_AND_TAG>>`. Limits are those in the certificate (single host, bar edge scoped out, cores by
binary equality, Kafka C converging, IDR-108, G12 harness shape, TEST_ONLY burst).

---

## Companion edit in the Trading System `INTEGRATION_DEFECT_REGISTER.md` (TS repo, same release transaction)

Rows whose `Fix / state` text still reads OPEN although the fix is in v2.2.4 (verified by reading the rows on
2026-10-17; the commits are from `git log 481fd67..HEAD`):

| ID | Register text now | Update to |
| --- | --- | --- |
| IDR-094 | "OPEN: pipelined commit ... in progress" | FIXED a962a5c + K1/K3 bcb0283; deployed: core rc22 `7c848aad` (2026-10-15 13:33Z); in v2.2.4 by binary equality |
| IDR-095 | "OPEN: measure after the writer read-ahead" | mitigation TCP_NODELAY in 98b421f (rc22); delay itself not measured; keep OPEN as DL_RISK |
| IDR-096 | "SDK FIXED bb75d06 ... Open: SDK/contract 2.4.1 release" | DL part released in v2.2.4 (gateway f5cd787, SDK 2.4.1 wheel `9f79e032...`); TS re-vendor open |
| IDR-099 | "FIXED in DL source, uncommitted" | FIXED 00056e5; deployed in python `c21e164a` (2026-10-16 17:14-17:18Z) |
| IDR-107 | "OPEN: classify non-fatal poll errors ..." | FIXED 58fb7cd + ff5683c; deployed in rust `60f83a51` projectors (2026-10-16 17:26/17:32Z) |
| IDR-108 | OPEN | stays OPEN; stated limit in the v2.2.4 certificate |
| IDR-090/091/092 | "not deployed (query image rc15)" | code in python `c21e164a` (2524732 is before REV); whether realm r16 / alpha-binance JWT rev 16 are live is NOT verified here (check the Query manifest revision before writing "deployed") |

(DL has no register file of its own: the DL plan points to the TS register, and `find` finds no
`INTEGRATION_DEFECT_REGISTER*` in the DL worktree.)

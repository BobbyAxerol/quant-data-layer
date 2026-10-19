# Amendment A4 v2 - the A4 rules pre-registered for the final stage-100 run (PRE_REGISTERED)

Status: **FROZEN, PRE-REGISTERED, not applied to any run yet.** Machine form:
[`config/v2/p184-stage100-a4-v2-preregistered.json`](../../../../config/v2/p184-stage100-a4-v2-preregistered.json)
(schema `qdl.p184.stage100-a4.v2`, version 2, `post_hoc: false`; its sha256 is in the plan journal entry of the commit
that froze it). Tool: `scripts/p184_a4_preregistered.py` (`bind` / `close` / `score`). Tests:
`tests/test_p184_a4_preregistered.py`.

## 1. Owner decisions

- AskUserQuestion 2026-10-18 ~16:40Z, answer "Duyệt A4 chốt trước (Recommended)": pre-register A4 v2 for the final
  stage-100 run. It is frozen before load and pinned to the run's runtime identity. Every other gate is unchanged. The
  verdict without A4 is reported next to the verdict with A4. A4 v2 adds **no** external exemption beyond A3.
- The rule content is exactly the A4 approved at ~14:45Z ([A4 v1](a4-post-hoc-ts-eligibility.md), section 2):
  - G4 `book_delta` / `book_snapshot` / `quote` `|OKX_INVERSE|`: gated by the pooled OKX row p95 limits
    289.1 / 270.1 / 283.3 ms on windows with n >= 20. The max is REPORT_ONLY. Every event above the replaced addendum
    limits 282.5 / 300.7 / 322.6 ms must be retained by observer v7, or the row is FAIL.
  - G2 `MARK_INDEX_PRICE|BINANCE`: max <= 1200 ms. p95 <= 500 ms is unchanged.
- A4 v1 (`config/v2/p184-stage100-a4-ts-eligibility.json`, sha256 `14958a68...`, POST_HOC_OWNER_DECISION) is not
  edited. It stays the decision for `p184-final-campaign-20261018T123222Z` only, and A4 v2 lists that run in
  `applies_to.excluded_campaigns`.

## 2. Why a wrapper and not an edit of the scorer

- A3 pins the sha256 of the scorer (`A3_CODE` includes `scripts/p184_gate_campaign.py`).
- The campaign preflight refuses a run whose working apparatus differs from the A3 pins. At score time a different tool
  drops A3, and with it the re-pinned G4-A1 / G4-A2.
- So the scorer stays byte-identical (sha256 `e179ec78...`). A4 v2 uses the same approach as A4 v1:
  - it runs the unchanged `build_scorecard`, which must reproduce the stored `scorecard.json`;
  - it runs it once more with `score_mark` / `score_g4` wrapped;
  - it refuses to write when any gate outside G2/G4 changes.
- The rule functions are the A4 v1 ones (`scripts/p184_a4_post_hoc.py`, sha256 pinned). A4 v2 only relabels their
  rows (`label: PRE_REGISTERED`, `post_hoc: false`, `amendment_version: 2`). The two versions therefore cannot judge
  the same data differently. The replay test checks this on 123222Z.

## 3. Runtime-identity pin

A3 hard-codes the image ids it saw at its freeze, and the campaign records the live ids at preflight
(`campaign.json`: `a3_runtime_images`, `runtime_identity`). The final run follows the rustls + resource roll, so its
image ids do not exist yet. A4 v2 hard-codes none. It binds to the run with two receipts written next to the campaign
directory:

| Step | Command | Writes (open "x", never overwritten) | Records |
| --- | --- | --- | --- |
| 1, before anything | `python3 -B scripts/p184_a4_preregistered.py bind --campaign-dir $C` (the launcher's first command, before the guard and `p184_gate_campaign.py run`) | `$C.a4-v2-binding.json` | A4 v2 sha256 and freeze time; sha256 of the scorer, the A4 rule module, the tool and the observer; image id of every A3 runtime role and the writer (same `docker inspect .Image` as `a3_runtime_images()`); writer identity in the preflight shape; cgroup caps (NanoCpus, CpuQuota, CpuPeriod, CpusetCpus, Memory, MemorySwap) of the A3 roles, kafka1-3 and the writer |
| 2 | `p184_gate_campaign.py run ... --campaign-dir $C` (unchanged) | the campaign | preflight identity in `campaign.json` |
| 3, after scorecard.json | `... p184_a4_preregistered.py close --campaign-dir $C` | `$C.a4-v2-close.json` | binding sha256; images and caps read again |
| 4 | `... p184_a4_preregistered.py score --campaign-dir $C --write` | `$C/scorecard.a4-v2.json` + `.sha256` | A4 v2 verdicts with `verdict_without_a4` per gate and `summary_without_a4` |

`bind` refuses when the A4 v2 file is invalid or not frozen yet, when a code pin differs from the working file, when
the campaign directory already exists, when the run is excluded, or when an image is missing or the writer is not
running.

At score time A4 v2 applies only when all of these hold:

- A4 v2 `frozen_at` < binding `created_unix` < campaign `started_unix`, and the A4 v2 file is unchanged since the bind.
- The preflight `runtime_identity` and `a3_runtime_images` equal the binding's.
- The close receipt names the binding, and its images and caps equal the binding's. So no roll and no `docker update`
  happened during the run. The G11 writer restart keeps both the image and the caps.
- The code pins hold, both now and at bind time, and the campaign `tool_sha256` is the pinned scorer.
- The campaign recorded the pinned budget, addendum, G4-A1 and G4-A2.
- The run is a 100-session candidate and is not excluded.
- The frozen scorer reproduces the stored `scorecard.json`.
- A3 was applied, the protocol was met, G6 is PASS, and G9 is PASS with 0 data-path eligibility losses.

If anything is missing, A4 v2 is not applied, and the stored strict `scorecard.json` is the record.

**A3 is not pinned by sha256.** Its `runtime_images` are hard-coded, and the preflight refuses other images
(`p184_gate_campaign.py:1793-1797`). So A3 has to be re-frozen for the rolled images before the final run can start.
A4 v2 requires only that the campaign applied the A3 it recorded, and the receipt reports that A3's path and sha256.

## 4. Tests (`tests/test_p184_a4_preregistered.py`)

- Negatives:
  - an inverse p95 over the pooled limit -> FAIL;
  - MARK BINANCE max 1200.1 ms or p95 500.1 ms -> FAIL (1200.0 PASS); MARK|OKX keeps max < 1000;
  - an invalid file: `post_hoc` true, the v1 label, an external exemption, a changed rule, applies_to or pin;
  - bind refusals: the directory exists, an image is missing, the file is frozen after the bind, a code pin differs,
    the run is excluded;
  - not applied: no binding, no close, a bind after the campaign start or before the freeze, A4 v2 edited after the
    bind, images or caps changed during the run, a preflight image or writer identity that differs, bind-time code
    that differs.
- Parity: without a binding nothing is applied. The frozen scorer reproduces the stored gates of 053803Z / 135719Z /
  074624Z / 092255Z / 123222Z (123222Z byte for byte in place). The frozen A4 v2 does not apply to 123222Z: it is
  excluded, and A4 v2 was frozen after that run started.
- Positive replay: 123222Z with a synthetic pre-run binding and a replay copy of A4 v2 (freeze time before the run, no
  exclusion). It gives the same summary as the post-hoc A4: G2 FAIL -> PASS, G4 FAIL -> PASS, the three inverse rows
  PASS, 8 outliers retained. G2/G4 content is equal apart from the version labels, and every other gate is identical.

## 5. Limits

- The rule content was chosen after observing 123222Z. A4 v2 makes it binding for a new run, but it is not evidence
  that the original frozen rule passes.
- The inverse window maximum is report-only.
- The MARK BINANCE tail split between DL Query and TS read scheduling is unknown (A4 v1 `limits_stated`).

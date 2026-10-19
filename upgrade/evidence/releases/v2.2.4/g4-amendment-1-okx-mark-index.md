# G4 amendment 1 (G4-A1): `mark_index_price|OKX|` scored on the same-event internal split

Status: **FROZEN before the candidate run** (frozen file `config/v2/p184-stage100-g4-amendment-1.json`,
sha256 `1c8a276f6cc6c03cdf768cdc9d31b65902bd849a1f3b7df56cda07ea47e6efa6`, frozen_at `2026-10-18T07:00:48Z`). Version 1.

- Decision owner: Bobby (owner).
- Approval: AskUserQuestion 2026-10-18 ~06:45Z, option "Amendment có bằng chứng + 1 run (Recommended)":
  one versioned G4 amendment for ONE row, then one clean final stage-100.
- RCA: `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md` commit `44fed4d` (2026-10-18 06:20-06:45Z, read-only);
  evidence `~/.local/state/qdl-v2/p184-g4-okx-mark-rca-20261018T062641Z/` (SHA256SUMS).
- Review input (recommendation, not approval): Codex
  `ts-codex-p184-review/upgrade/p184-idr112-113-provider-gates-review-20261018.md`, "Gate Amendment Options".

## Scope

Applies to exactly one row of the frozen G4 table: `mark_index_price|OKX|`. Unchanged: every other G4 row (frozen
table and the 2026-10-11 addendum), every other gate G1-G14, every threshold, the observer sampler and population, the
window schedule. The budget (`ff85335b...`) and the addendum (`4ed59dca...`) are byte-identical. Old receipts are
immutable: the scorer applies the amendment only to a campaign that recorded its sha256 at run time, so
`p184-final-campaign-20261018T053803Z`, `...20261017T135719Z` and every earlier campaign keep their verdicts (test
`ExistingReceiptParityTests`: the re-scored G4 gate is byte-identical to the stored scorecard).

## Why (verified in code and data)

The row is a PAIRED mark+index event. `rust/qdl-realtime-core/src/lib.rs` `process_mark_index` (908-1145) emits on
every changed component with source_event_time = min(mark.ts, index.ts) (1044), received_at_ns = the OLDEST component
receipt (1042, 1081) and normalized/published_at_ns = the NEWEST (1043, 1082-1083); source_sequence carries
`mark_ts:index_ts:mark_receipt_ns:index_receipt_ns` (1046-1054). producer_ts is librdkafka CreateTime at the core's
transactional send (`rust/qdl-kafka/src/lib.rs` 1136-1141). OKX pushes index-tickers only on change (else once a
minute), mark-price every 200 ms on change. So the frozen quantity (observer receipt - min(ts)) for this row is
dominated by the quiet age of the index carried by the pair, a venue/market-regime quantity; the DL-internal part is
CreateTime - newest component receipt (RCA: p95 81-105 ms in every candidate window, baseline 88-147 ms, raw samples).

## Rule

Verdict quantity (observer v6 `paired_split`, per ADMITTED event of the unchanged v2 sampler population):
`internal_newest_receipt_to_producer_ts_ms = CreateTime(ms) - max(mark_receipt_ns, index_receipt_ns)`.

PASS needs, in every G4 window with samples:
- p95 (frozen-table shape): internal-split p95 <= **200.9 ms** (= recomputed baseline p95 167.4 x 1.2);
  the observer p95 is a bucket upper bound, a bucket straddling the limit is unresolved (NOT_MEASURED);
- max (addendum shape): on windows with >= 5 samples, internal-split max <= **993.4 ms** (= recomputed baseline
  max 827.8 x 1.2);
- attribution: a covering venue-direct probe in that window (EXTERNAL_VENUE_COMPONENT_CADENCE). A window without one
  is UNKNOWN, and UNKNOWN is not EXTERNAL: the row is NOT_MEASURED.
A decisive excess in any window is FAIL. Events without a parsable split, without CreateTime, or with published_at !=
newest receipt make the row NOT_MEASURED (the premise of the split would be broken). No window with samples is
NOT_MEASURED.

Report-only (never scored, all outliers kept): the composite visibility value per window with the frozen limit
1586.6 ms and whether it would exceed it, the top-5 composite outlier capsule over every accepted event; the component
ts gap |mark.ts - index.ts|, component receipt gap, per-component venue -> DL, older component and the internal-split
outlier capsule; the venue probe per window (`scripts/p184_g4_okx_mark_index_probe.py`: one public WS, mark-price +
index-tickers for the six DL instruments, cadence, recv - venue ts, venue-only pair gap at each push) with the host
clock bound from `chronyc -n tracking` (|offset| + root dispersion + root delay / 2) before and after.

## Recomputed baseline (frozen G4 table baseline run)

Source: `~/.local/state/qdl-v2/p184-d3-baseline-20261009` (the budget's `sources.g4` = the four summary sha256).
Method: the same records the frozen table was computed from. The composite p95 recomputed per window with the
observer's v1-v3 summary equals the frozen `baseline_p95_ms_windows` exactly; the internal split of those same records
uses the frozen table's exact nearest rank (index int(0.95 n)); max exact. Every record parsed, carried CreateTime and
had published_at == newest receipt (otherwise the freeze refuses: insufficient baseline).

| window | records | composite p95 (frozen) | internal p50 | internal p95 | internal max | component ts gap p95 / max |
|---|---|---|---|---|---|---|
| 1 | 662 | 1322.2 | 99.9 | 167.4 | 292.1 | 1134.0 / 1954.0 |
| 2 | 662 | 1289.4 | 75.9 | 143.8 | 245.8 | 1168.0 / 1995.0 |
| 3 | 664 | 1296.2 | 75.5 | 139.1 | 182.5 | 1124.0 / 2016.0 |
| 4 | 662 | 1195.2 | 73.9 | 138.2 | 827.8 | 1093.0 / 2056.0 |

Baseline p95 = max of window p95 = 167.4 ms -> limit 200.9 ms; baseline max = 827.8 ms -> limit 993.4 ms. Source sha256: `g4-1-summary.json` 09bd7219c41e..., `g4-1/observations.jsonl` 42bbff0fa4ea..., `g4-2-summary.json` 83c8ab8c3630..., `g4-2/observations.jsonl` 3edad237d35e..., `g4-3-summary.json` f7174be2ebea..., `g4-3/observations.jsonl` ca237ba9bdf2..., `g4-4-summary.json` a902171d065a..., `g4-4/observations.jsonl` 4c7c6b87d091...

## Apparatus hashes (old = base ref 44fed4d -> new = frozen with the amendment)

| file | old sha256 (44fed4d) | new sha256 |
|---|---|---|
| `config/v2/p184-stage100-budget.json` | ff85335b3c8762d9acd8e7230f3c81ee1ec8403aff34cf2fb8f67714521c2541 | ff85335b3c8762d9acd8e7230f3c81ee1ec8403aff34cf2fb8f67714521c2541 (unchanged) |
| `scripts/p184_freeze_g4_amendment.py` | (new file) | d0ec263b414c71c2c5b97b9f7d471d8c14529593dd1e141d2af2375533364a35 |
| `config/v2/p184-stage100-g4-addendum.json` | 4ed59dca0212566d3ff03c5a4c23eed3d252d0150844b9a4fd6fa543333d78b5 | 4ed59dca0212566d3ff03c5a4c23eed3d252d0150844b9a4fd6fa543333d78b5 (unchanged) |
| `scripts/p184_g4_observer.py` | ff4f59898b5023801ca67df2da7d72a1fea16b7f1935664fec81e07cb236f7e0 | 5d90cc60cc1ebad783d6bec848fe7017918f783e3ad0dfcfaa2aed1f47fcba74 |
| `scripts/p184_gate_campaign.py` | cf26d6e31b5dfcaa5572be6304943380542e6b292275ce799268a619e85bde98 | 69c26d729d77d9b152bc8b4df86077827689dfc9b0d15fb290abfcdec64835fb |
| `scripts/p184_g4_okx_mark_index_probe.py` | (new file) | ee835d9c38bf6fe7b3410a21e32cadddd154af048156797efdce0cbd68a91e41 |

The scorer applies the amendment only when the campaign's tool sha256 equals the pinned new gate-campaign tool.

## What this does not claim

Recommended certificate wording if the run passes: the internal DL path of the OKX mark/index pair is qualified
against its recomputed baseline; the composite value (the venue index quiet age carried by the pair) is observed and
reported under amendment G4-A1, not certified as a latency bound. Not an unconditional FULL, not a statement about
other rows or venues.

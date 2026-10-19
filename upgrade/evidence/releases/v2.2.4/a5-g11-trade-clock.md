# Amendment A5: G11 TRADE first-valid-apply clock (POST_HOC_OWNER_DECISION for 040311Z; pre-registered after)

Status: frozen 2026-10-19T05:03:03Z. Applied post hoc to `p184-final-campaign-20261019T040311Z` only, and
pre-registered for every stage-100 campaign that starts after the freeze. The verdict without A5 stays the record of
the frozen rule and is shown next to the A5 verdict in every release surface.

| Item | Path | sha256 |
| --- | --- | --- |
| Amendment file | `config/v2/p184-stage100-a5-g11-trade-clock.json` | `c5b46c5faefe20e31061a3bd647b9e60537b982eee43b484715ec9cdcc5aae78` |
| Rescore tool (wrapper, the frozen scorer is not edited) | `scripts/p184_a5_g11_trade_clock.py` | `4892abd1630eff84c1a3b6eb0b57b3c57912883cbd354378d686e67ab89af7e7` |
| Tests | `tests/test_p184_a5_g11_trade_clock.py` | `3211f1ff403df65d1f5c1abeb7bc59f156d2641a41f1a77ae1595af83497a70b` |
| Frozen scorer (= the A3 pin) | `scripts/p184_gate_campaign.py` | `e179ec784621089795516ed25b2ea1f2e2ba30c86eea0de852ed4728aa6d104b` |
| Frozen G11 probe / oracle | `scripts/p184_g11_writer_restart_probe.py` | `adb811ccc6da8a7249197e5e8e22fb49deebee0c03f36ded71c3c84838bf3e95` |
| Rescore receipt | `~/.local/state/qdl-v2/p184-final-campaign-20261019T040311Z/scorecard.a5.json` | `b1c2e7b450363f094c25adb34988f9356da327ef3d0b6a863a001dab2692d08d` |
| Stored scorecard (frozen rule, untouched) | same directory, `scorecard.json` | `ebe11b8c10145bb92b7a850c4134d81101aaf7000a210f198b175316b20acbbf` |

## 1. Decision

- Owner rule, 2026-10-18 ~16:00Z (verbatim): "nếu gate không đạt xác định là lỗi external thì nới gate rồi release
  gọn nhé". With it: "duyệt hết theo recommend của bạn, k hỏi lại". The coordinator recommended this amendment, so no
  further question was asked.
- A5 was adopted **after** 040311Z was scored, so for that run it is `POST_HOC_OWNER_DECISION` (`post_hoc: true`).
  The same file pre-registers the rule for later campaigns (`PRE_REGISTERED`, `post_hoc: false`).

## 2. What the frozen rule measured on 040311Z

G11 (frozen 2026-10-09): "fault cleared -> first valid apply per slice <= 30 s, caught up <= 60 s; no rewind; cursor
never ahead". On 040311Z it FAILs on exactly one of 64 slices:

- `OKX|BNB-USDT-SWAP|TRADE|None`: first valid apply 36.415 s, caught up 36.415 s (within 60 s).
- Every other slice: <= 4.928 s. Outage 0.113 s. Rewinds 0. Cursor never ahead (3 samples, 21 of 21 compared).

A TRADE slice can only be applied when the venue prints a trade. So on a quiet instrument the frozen clock times the
venue's silence, not the writer's recovery. Evidence for this run (read-only, in
`~/.local/state/qdl-v2/p184-final-campaign-20261019T040311Z.g11-forensic/`):

- `md.canonical.v2`, read_committed, assign only, no group join or commit, all 6 partitions finished, no read error
  (`canonical.jsonl` `cf96554e...`, reader `reader.py` `5fcc088a...`). The read covered 04:35:20Z..04:38:00Z.
- On partition 2, connection generation 210:
  - the last OKX BNB-USDT-SWAP trade before the fault is id 241971262 (venue 04:36:15.209Z, Kafka 04:36:15.273Z);
  - the next one is id 241971263 (venue 04:36:57.139Z, DL receive +28 ms, Kafka CreateTime 04:36:57.191Z);
  - no BNB-USDT-SWAP trade exists in between (41.9 s).
- The OKX public REST history returns the same two ids as adjacent trades
  (`okx-history-trades-after-241971264.json` `b579fdb9...`). The venue printed nothing in between.
- The writer rewrote the TS cache key at 04:36:57.604Z (G11 poll, upper bound one 500 ms poll), carrying event
  04:36:57.139Z: 0.413 s after the DL Kafka time and 0.465 s after the venue time.

Fault cleared at 04:36:21.189Z; the first trade came 36.0 s later. That is EXTERNAL_QUIET under the owner's rule.

## 3. The rule

For G11 only, and for TRADE slices only:

1. The clock of the slice starts at `max(fault_cleared, CreateTime of the slice's first canonical TRADE event whose
   CreateTime is later than outage_start - 3000 ms)`.
2. The A5 first valid apply = (first apply observed by the frozen probe - clock start) / 1000. The limit stays 30 s.
3. The 3000 ms guard is the G11 TRADE live bound. An event committed less than 3 s before the writer stopped may not
   have been applied by the old process; it starts the clock at `fault_cleared`, the frozen behaviour.
4. CreateTime (set by DL before its Kafka transaction commits) is the earliest moment the writer could read the
   event. So the A5 interval is an upper bound of writer recovery.

Evidence required per slice. Without it the slice keeps the frozen clock (A5 not applied to it):

- a read of `md.canonical.v2` by the pinned reader;
- every assigned partition finished and no read error;
- on every partition that carries the slice, records from at or before `outage_start - 3000 ms` and from at or after
  the first apply (continuous coverage);
- the event the probe saw applied first (its `event_ms`) must be one of the canonical post-fault TRADE events of that
  slice.

Unchanged: caught up <= 60 s measured from `fault_cleared`; no rewind (high-water); cursor never ahead with a complete
roster; unresolved slices and the observation window; every non-TRADE slice; every other gate. The tool rewrites only
the TRADE first-apply values in a copy of the probe report and then calls the frozen `verdict()`.

What makes A5 FAIL, not pass:

- an earlier post-fault TRADE event that the writer did not apply within 30 s moves the clock back to that event;
- an event inside the 3 s guard keeps the frozen clock;
- incomplete or non-matching evidence keeps the frozen clock.

## 4. Implementation (why a wrapper)

A3 pins the sha256 of the scorer it runs with. An edited `scripts/p184_gate_campaign.py` would drop A3 (and with it the
re-pinned G4-A1/G4-A2) from any rescore. So `scripts/p184_a5_g11_trade_clock.py`:

- runs the unchanged `build_scorecard` once; it must reproduce the stored `scorecard.json`;
- runs it again with the probe's `verdict()` wrapped for this campaign;
- raises if any gate other than G11 changes, and then writes nothing;
- writes `scorecard.a5.json` and its `.sha256` with open("x"), never overwriting.

For the post-hoc application it also checks the pinned sha256 of `scorecard.json`, `g11-summary.json`,
`g11-observations.jsonl`, the canonical evidence and the reader. A run that A5 does not name and that started before
the freeze is refused (not pre-registered). Later runs put their evidence in `<campaign>.g11-canonical/` (`reader.py`,
`canonical.jsonl`, `a5-evidence.json`).

## 5. Result on 040311Z (receipt `b1c2e7b4...`)

| | Without A5 (frozen rule) | With A5 |
| --- | --- | --- |
| G11 | **FAIL**: first valid apply max 36.415 s > 30 s | **PASS** |
| OKX BNB-USDT-SWAP TRADE first valid apply | 36.415 s (from fault cleared) | 0.413 s (from its first post-fault event, CreateTime 04:36:57.191Z) |
| First valid apply over 64 slices | max 36.415, p95 4.928 | max 4.928, p95 4.407 |
| Caught up / rewinds / cursor | max 36.415 s / 0 / never ahead | unchanged |
| Other gates | PASS G1-G10, G14 | byte-identical |
| Summary (campaign) | PASS 11, FAIL 1 (G11), NOT_MEASURED 2 (G12/G13, separate runs) | PASS 12, NOT_MEASURED 2 |

A5 was applied to one slice. The other 9 TRADE slices have no canonical evidence, so they keep the frozen clock; all of
them are <= 2.408 s.

## 6. Tests (`tests/test_p184_a5_g11_trade_clock.py`, 16 tests, host)

- Negatives:
  - an earlier unapplied post-fault event -> FAIL (34.415 s);
  - an event during the outage or inside the 3 s guard -> frozen clock;
  - missing evidence, no counts record, unfinished partition, read error, coverage starting inside the guard or ending
    before the apply, no post-fault trade, or an applied event that is not in the read -> not applied;
  - changed or missing pinned evidence or reader -> not applied;
  - a changed A5 file (11 variants) -> refused;
  - an unnamed run started before the freeze -> refused.
- Unchanged checks: caught up > 60 s, rewinds, cursor ahead and a 31 s BOOK slice still FAIL with A5.
- Parity without A5 evidence:
  - on all 29 stored `g11-summary.json` files, the verdict and recovery equal the frozen oracle;
  - on 6 stored receipts (053803Z, 135719Z, 074624Z, 092255Z, 123222Z, 021044Z), every gate and the summary are
    unchanged.
- 040311Z: the stored scorecard is reproduced in place; only G11 and only the BNB TRADE slice change.

## 7. Limits

- A5 is post hoc for 040311Z. It moves a clock after measuring, by a recorded owner rule, and it does not change the
  frozen 30 s or 60 s limits.
- Venue silence is proven from the DL canonical topic and the venue's public trade history. It is not inferred from
  writer behaviour. The writer's 36.4 s of `NOT_EXECUTION_ELIGIBLE` polls on that key are the correct at-use semantics
  of a stale last trade (quality `LAST_EVENT_STALE`).
- On 040311Z the evidence covers one slice, the only one over 30 s. It was produced after the run by the forensic
  reader, whose output does not record its own read window. The window is in the DL plan journal (commit `288a5805`),
  and the tool proves coverage from the records themselves.

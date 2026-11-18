# DL v2.4.0 certificate (DRAFT, NOT ISSUED)

**Status.** Candidate only. Nothing below is production evidence until the rollout (`ROLLOUT_PLAN.md`) and the
shared100 run have receipts. Release head and images: see section 3.

## 1. Scope

| Item | Change | Evidence | Status |
|---|---|---|---|
| v2.3.2 content | Stream SessionAlive frames, opt-in DELIVERY_ON_CHANGE, SDK 2.7.0, R0 recipe | `../../v2.3.2/` (A-WR), merged `89697be` | tested locally (v2.3.2) |
| S1 verified-token cache | LRU 4096, until `exp`, key rotation evicts | `tests/test_dl_v240_identity_quota.py` | tested locally |
| S2 advisory token revision | readers accept 1..manifest revision; files unchanged | same, `tests/test_v230_declaration_mechanism.py`, `tests/test_dl_v240_realm_composition.py`, Rust `gateway_bundle` | tested locally |
| S3 quota only on admission | settlement middleware, cost notes, Rust refund guard | identity_quota REST tests, Rust `quota_guard_tests` | tested locally |
| S4 direct MARK/INDEX view call | shared endpoint function, `InProcessViewClient`, cached capability profile | `tests/test_dl_v240_shared_reads.py` A/B equivalence | tested locally |
| S5 shared latest view | singleflight per (binding, last), TTL 10 ms in runtime | shared_reads S5 tests | tested locally |
| S6 session-fair warmup queue + warmup singleflight; hot lanes session-fair | `FairWarmupLane`, `_WarmupFlights`, waits capped below client timeouts | shared_reads storm/noisy/abandon tests; updated lane tests | tested locally |
| S7 local token buckets | per replica rpm/2 + burst, TS full rate, session share 1/8, optional Redis | identity_quota + Rust auth tests | tested locally |
| S8 role claims ignored | manifest permissions only gate | identity_quota, phase7 security, Rust native_stream | tested locally |
| SDK 2.8.0 | `session_id` (X-QDL-Session), warmup timeout >= deadline + 2 s | identity_quota SDK tests, `tests/test_qdl_sdk_release.py` | tested locally |
| S9 shared100 | profile, scorer, capacity candidate, A6 | `tests/test_dl_v240_shared100.py` | prepared, not run |

## 2. Local preflight (filled from the journal entry "DL v2.4.0 S1-S9")

| Job | Reproduction | Result |
|---|---|---|
| Python impacted set | `qdl-v2-python:2.3.1-aa75218`, `--cpus 1 --network none`, tree read-only | 1470 run; failures fixed; reruns 191 (1 fixed), 262 OK, 43 OK |
| Rust qdl-contracts + qdl-stream-gateway | `qdl-rust-builder:1.82.0-v2.3.2`, offline, uid 1001 | fmt 0, clippy -D warnings 0, test 187 / 0 / 5 ignored |
| Full CI | PR to dev (see `PR_BODY.md`) | OPEN |
| R0 security (cargo-deny, trivy, pip-audit) | on the two R0 image ids | OPEN (scheduled by the coordinator) |

## 3. Release candidate

- Source: `release/v2.4.0` head `<HEAD>` (recipe commit 56eb6b3).
- rust `qdl-v2-rust:2.4.0-<sha7>` `<RUST_DIGEST>`; python `qdl-v2-python:2.4.0-<sha7>` `<PYTHON_DIGEST>`; receipt
  `~/.local/state/qdl-v2/build-v2.4.0-<sha7>/receipt.json`.

## 4. Gates to issue the certificate

1. Full CI green on the release tree (CI6), or an inherited run on the identical tree.
2. R0 security gates PASS on the two image ids.
3. Rollout steps 1-7 with receipts, every gate PASS (or rolled back and re-run).
4. shared100 (A6) scored PASS: restart storm, noisy neighbour, G14 alpha delta <= 0.5 vCPU (owner-approved).

## 5. Micro-bench (offline, not production evidence)

S1 280.6 -> 4.9 us; S4 924.0 -> 647.8 us; S5 3,924 -> 99 market-cache reads for 3,924 / 5,258 requests; S6 storm 80
served / 560 refusals / 20.8 s -> 100 / 0 / 2.0 s; 100 identical warmups -> 1 materialization.

## 6. Known limits

- The hot lanes have no client-disconnect probe (their wait is capped at 7.5 s instead).
- Rust per-method refusals are refunded; a stream that later ends is not (one unit per open, as before).
- A6's G14 limit is owner-approved; the other shared100 numbers are the owner's stated shape.
